import streamlit as st
import os
import math
import mysql.connector
import pymongo
import time

from pymongo import MongoClient
from dotenv import load_dotenv
from pprint import pprint
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime

from cache import app

mongo_client = None
# app = None

def mongo_db_connection():
    global mongo_client
    if mongo_client is None:
        load_dotenv()
        mongo_username = os.environ.get("MONGOUSERNAME")
        mongo_password = os.environ.get("MONGOPASSWORD")
        uri = "mongodb+srv://{}:{}@twitter.qlewowk.mongodb.net/?retryWrites=true&w=majority&appName=twitter".format(mongo_username, mongo_password)
        mongo_client = MongoClient(uri, server_api=ServerApi('1'))
        try:
            mongo_client.admin.command('ping')
            print("Connected to MongoDB!")
        except Exception as e:
            print(e)
    return mongo_client

def mysql_db_connection():
    load_dotenv()
    sql_username = os.environ.get("SQLUSERNAME")
    sql_password = os.environ.get("SQLPASSWORD")

    conn = mysql.connector.connect(
        host="localhost",
        user=sql_username,
        password=sql_password,
        database="twitter_user_data"
    )
    print("Connected to MySQL!")
    return conn

def get_user_info(user_id):
    conn = mysql_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = "SELECT * FROM users_info WHERE id = %s"
    cursor.execute(query, (user_id,))
    user_info = cursor.fetchone()
    conn.close()
    return user_info

def on_close():
    global app
    print("Shutting down")
    app.shutdown()


def main():
    global app
    st.title("Tweet Search Engine")
    
    current_page = st.experimental_get_query_params().get("page", ["search"])[0]

    if current_page == "search":
        search_page()

    elif current_page == "results":
        results_page()
    # If on user_info page
    elif current_page == "user_info":
        username = st.experimental_get_query_params().get("username", [None])[0]
        if username:
            user_info_page(username)
        else:
            st.error("No username provided for user_info page.")


def search_page():
    st.subheader("Advanced Search Options")
    
    with st.form(key='search_form'):
        input_keyword = st.text_input("Enter Keyword (Optional)")
        input_hashtag = st.text_input("Enter Hashtag (Optional)")
        input_language = st.selectbox("Select Language", ["Select","en", "fr", "ge", "in"], index=0)
        
        # User search input
        user_search = st.text_input("Search for a user by username")

        form_submit_button = st.form_submit_button(label='Search')

    if form_submit_button:
        if user_search:
            if not (input_keyword or input_hashtag or input_language != "Select"):
                st.experimental_set_query_params(page="user_info", username=user_search)
            else:
                st.experimental_set_query_params(page="user_info", username=user_search, keyword=input_keyword, hashtag=input_hashtag, language=input_language)
        else:
            st.experimental_set_query_params(page="results", keyword=input_keyword, hashtag=input_hashtag, language=input_language)
        st.experimental_rerun()


def get_mongo_query(collection, input_keyword=None, input_hashtag=None, input_language="Select"):
    db = mongo_client.sample_test
    collection = db.tweets_test
    top_tweets = []

    query = {"retweeted_status": {"$exists": False}}
    if input_keyword:
        query["text"] = {"$regex": input_keyword, "$options": "i"}
    if input_hashtag:
        query["entities.hashtags.text"] = {"$regex": input_hashtag, "$options": "i"}
    if input_language != "Select":
        query["lang"] = input_language
    try:
        original_tweets = collection.find(query).sort([("retweet_count", -1), ("favorite_count", -1)])

        for original_tweet in original_tweets:
            top_tweets.append(original_tweet)
            if len(top_tweets) == 50:
                break
    except Exception as e:
        print(e)

    return top_tweets


# +
def results_page(): 
    global app
    
    start_time = time.time()
    input_keyword = st.experimental_get_query_params().get("keyword", [""])[0]
    input_hashtag = st.experimental_get_query_params().get("hashtag", [""])[0]
    input_language = st.experimental_get_query_params().get("language", ["en"])[0]

#     db = mongo_client.sample_test
#     collection = db.tweets_test
#     query = {"retweeted_status": {"$exists": False}}
    try:
        top_tweets = []
        if input_keyword:
            top_tweets += app.search_cache("tweet", input_keyword, input_language)
        if input_hashtag:
            top_tweets += app.search_cache("hashtag", input_keyword, input_language)

        num_pages = math.ceil(len(top_tweets) / 10)
        page_number = st.number_input("Page Number", min_value=1, max_value=num_pages, value=1, key="page_number")
        tweets_per_page = 10
        
        display_tweets(top_tweets, page_number, tweets_per_page)
    except Exception as e:
        st.error(f"Error occurred while searching MongoDB: {e}")


# -

def display_tweets(tweets, page_number, tweets_per_page):
    global app
    # db = mongo_client.sample
    # collection = db.tweets
    print(app.cache["tweet"].keys())
    st.write(f"## Page {page_number}")
    start_index = (page_number - 1) * tweets_per_page
    end_index = min(page_number * tweets_per_page, len(tweets))
    for i in range(start_index, end_index):
        original_tweet = tweets[i]
        user_id = original_tweet['user_id']
        user_info = app.search_cache("user", user_deets = [user_id, None])
        
        user_name = user_info.get('name', 'Unknown') if user_info else 'Unknown'
        user_screen_name = user_info.get('screen_name', 'Unknown') if user_info else 'Unknown'
        user_display = f"{user_name} ([@{user_screen_name}](?page=user_info&username={user_screen_name}))"

        # Display the original tweet's text along with the user name
        original_tweet_text = original_tweet.get('text')
        st.write("---")
        st.write(f"### Tweet by {user_display}")
        st.write(original_tweet_text)
        # Display quoted status if available
        quoted_status = original_tweet.get("quoted_status")
        if quoted_status:
#             quoted_user_info = get_user_info(quoted_status['user_id'])
            quoted_user_info = app.search_cache("user", user_deets = [quoted_status['user_id'], None])
            quoted_user_name = quoted_user_info.get('name', 'Unknown') if quoted_user_info else 'Unknown'
            quoted_user_screen_name = quoted_user_info.get('screen_name', 'Unknown') if quoted_user_info else 'Unknown'
            quoted_user_display = f"{quoted_user_name} ([@{quoted_user_screen_name}](?page=user_info&username={quoted_user_screen_name}))"
            quoted_tweet_text = quoted_status.get('text', 'No text available')
            st.write(f">>> Original Tweet by {quoted_user_display}: {quoted_tweet_text}")
        # Create columns to display retweet count and favorite count alongside the tweet
        retweet_count = original_tweet.get("retweet_count", 0)
        favorite_count = original_tweet.get("favorite_count", 0)
        col1, col2 = st.columns([1, 1])
        with col1:
            st.write(f"\U0001F501  {retweet_count}")  # Retweet symbol
        with col2:
            st.write(f"\U00002764  {favorite_count} ")  # Red heart emoji
      
        # Display retweets
        retweets = original_tweet.get("retweets", [])
        retweet_count = len(retweets)
        if retweet_count == 0:
            st.info("No retweets.")
        else:
            retweet_user_names = []
            for i, retweet in enumerate(retweets):
                if i >= 30:
                    break  # Exit loop after processing 30 retweets
#                 user_info = get_user_info(retweet.get('user_id'))
                user_info = app.search_cache("user", user_deets = [retweet.get('user_id'), None])
                print(app.cache["user"].keys())
                if user_info:
                    user_name = user_info.get('name', 'Unknown')
                    user_screen_name = user_info.get('screen_name', 'Unknown')
                    user_display = f"{user_name} ([@{user_screen_name}](?page=user_info&username={user_screen_name}))"
                    retweet_user_names.append(user_display)
            if retweet_user_names:
                st.info(f"Retweeted by: {', '.join(retweet_user_names)}")
    end_time = time.time()
    st.write("Search Results in {} seconds".format(end_time - start_time))     


def user_info_page(username, keyword=None, hashtag=None, language="Select"):
    global app
    # Connect to MySQL and fetch user information
    input_keyword = st.experimental_get_query_params().get("keyword", [""])[0]
    input_hashtag = st.experimental_get_query_params().get("hashtag", [""])[0]
    input_language = st.experimental_get_query_params().get("language", ["Select"])[0]
    try:
        user_info = app.search_cache("user", user_deets = [None, username])

    except Exception as e:
        st.error(f"Error occurred while fetching user information from MySQL: {e}")
        return

    if user_info:
        user_id = str(user_info.get('id'))
        user_name = user_info.get('name', 'Unknown')
        user_screen_name = user_info.get('screen_name', 'Unknown')
        user_location = user_info.get('location', 'Unknown')
        user_description = user_info.get('description', 'Unknown')
        verified = user_info.get('verified', False)
        followers_count = user_info.get('followers_count', 0)
        friends_count = user_info.get('friends_count', 0)
        created_at = user_info.get('created_at', 'Unknown')

        # Display user information
        st.write(f"**Full Name:** {user_name} (@{user_screen_name}) {'✓' if verified else ''}")
        st.write(f"**Location:** {user_location}")
        st.write(f"**Description:** {user_description}")
        st.write(f"**Followers Count:** {followers_count}")
        st.write(f"**Friends Count:** {friends_count}")
        st.write(f"**Created At:** {created_at}")
        st.write("---")

        st.write("### TWEETS")
        try:
            user_tweets = app.tweets_for_users(user_id, input_keyword, input_hashtag, input_language)
            tweets = list(user_tweets)
            print(len(tweets))
            
            if not tweets:
                st.warning("No tweets found.")
                return
            # Display tweets
            for tweet in tweets:
                st.write("---")
                st.write(f"**{tweet['text']}**")
                # Display quoted tweet if available
                quoted_status = tweet.get("quoted_status")
                if quoted_status:
                    quoted_tweet_text = quoted_status.get('text', 'No text available')
                    st.write(f">>> Quoted Tweet: {quoted_tweet_text}")
                retweet_count = tweet.get("retweet_count", 0)
                favorite_count = tweet.get("favorite_count", 0)
                col1, col2 = st.columns([1, 1])
                with col1:
                    st.write(f"\U0001F501  {retweet_count}")  # Retweet symbol
                with col2:
                    st.write(f"\U00002764  {favorite_count} ")  # Red heart emoji
                                
                # Display retweets
                retweets = tweet.get("retweets", [])
                retweet_count = len(retweets)
                retweet_user_names = []
                for i, retweet in enumerate(retweets):
                    if i >= 30:
                        break  # Exit loop after processing 30 retweets
                    user_info = app.search_cache("user", user_deets = [retweet.get('user_id'), None])
                    if user_info:
                        user_screen_name = user_info.get('screen_name', 'Unknown')
                        user_display = f"[@{user_screen_name}](?page=user_info&username={user_screen_name})"
                        retweet_user_names.append(user_display)
                if retweet_user_names:
                    st.info(f"Retweeted by: {', '.join(retweet_user_names)}")
        except Exception as e:
            st.error(f"Error occurred while fetching user tweets from MongoDB: {e}")
    else:
        st.error(f"No user found with username '{username}' in MySQL.")


if __name__ == "__main__":
    main()
