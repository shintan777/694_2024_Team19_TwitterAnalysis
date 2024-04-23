import streamlit as st
import pymongo as pm
import json
import os
import math
import mysql.connector
from pymongo import MongoClient
from bson import ObjectId
from dotenv import load_dotenv
from datetime import datetime
from pprint import pprint
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime
import time

def mongo_db_connection():
    load_dotenv()
    mongo_username = os.environ.get("MONGOUSERNAME")
    mongo_password = os.environ.get("MONGOPASSWORD")
    uri = "mongodb+srv://{}:{}@twitter.qlewowk.mongodb.net/?retryWrites=true&w=majority&appName=twitter".format(mongo_username, mongo_password)
    client = MongoClient(uri, server_api=ServerApi('1'))
    try:
        client.admin.command('ping')
        print("Connected to MongoDB!")
    except Exception as e:    
        print(e)
    return client

def mysql_db_connection():
    load_dotenv()
    sql_username = os.environ.get("SQLUSERNAME")
    sql_password = os.environ.get("SQLPASSWORD")

    conn = mysql.connector.connect(
        host = "localhost",
        user = sql_username,
        password = sql_password,
        database = "twitter_user_data"
    )
    print("Connected to MySQL!")
    return conn

def get_user_info(user_id):
#     conn = mysql_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = "SELECT * FROM users_info WHERE id = %s"
    cursor.execute(query, (user_id,))
    user_info = cursor.fetchone()
    conn.close()
    return user_info


def main():
    st.title("Tweet Search Engine")

    # Connect to MongoDB and fetch the collection
    mongo_client = mongo_db_connection()
    mongo_db = mongo_client.sample
    mongo_collection = mongo_db.tweets
    
    conn = mysql_db_connection()

    current_page = st.experimental_get_query_params().get("page", ["search"])[0]

    if current_page == "search":
        search_page(mongo_collection, conn)

    elif current_page == "results":
        results_page(mongo_collection, conn)

    elif current_page == "user_info":
        username = st.experimental_get_query_params().get("username", [None])[0]
        if username:
            user_info_page(username, mongo_collection, conn)
        else:
            st.error("No username provided for user_info page.")



def search_page(mongo_collection, conn):
    st.subheader("Advanced Search Options")
    input_keyword = st.text_input("Enter Keyword (Optional)")
    input_hashtag = st.text_input("Enter Hashtag (Optional)")
    input_language = st.selectbox("Select Language", ["en", "fr", "ge", "in", "Other"], index=0)
    
    # User search input
    user_search = st.text_input("Search for a user by username")

    # When search button is clicked, navigate to results page with search parameters
    if st.button("Search"):
        if user_search:
            # Navigate to user info page
            st.experimental_set_query_params(page="user_info", username=user_search)
        else:
            # Set URL params for results page
            st.experimental_set_query_params(page="results", keyword=input_keyword, hashtag=input_hashtag, language=input_language)


def results_page(mongo_collection, conn):
    print("********")
    print(keyword, hashtag, language)
    print("********")
    
    # Get search parameters from URL params
    input_keyword = st.experimental_get_query_params().get("keyword", [""])[0]
    input_hashtag = st.experimental_get_query_params().get("hashtag", [""])[0]
    input_language = st.experimental_get_query_params().get("language", ["en"])[0]

    # Connect to MongoDB and fetch results based on search parameters
#     mongo_client = mongo_db_connection()
#     db = mongo_client.sample
#     collection = db.tweets

    # Define the MongoDB query based on search parameters
    query = {"retweeted_status": {"$exists": False}}
    if input_keyword:
        query["text"] = {"$regex": input_keyword, "$options": "i"}
    if input_hashtag:
        query["entities.hashtags.text"] = {"$regex": input_hashtag, "$options": "i"}
    if input_language != "Other":
        query["lang"] = input_language

    try:
        original_tweets = collection.find(query).sort([("retweet_count", -1), ("favorite_count", -1)])

        # Collect top 50 tweets
        top_tweets = []
        for original_tweet in original_tweets:
            retweet_exists = collection.find_one({"retweeted_status.id_str": original_tweet["id_str"]})
            if retweet_exists:
                top_tweets.append(original_tweet)
                if len(top_tweets) == 50:
                    break

        # Calculate number of pages
        num_pages = math.ceil(len(top_tweets) / 10)

        # Pagination
        page_number = st.number_input("Page Number", min_value=1, max_value=num_pages, value=1, key="page_number")
        tweets_per_page = 10

        # Display tweets for the selected page
        display_tweets(top_tweets, page_number, tweets_per_page)
    except Exception as e:
        st.error(f"Error occurred while searching MongoDB: {e}")

def user_info_page(username, mongo_collection):
    conn = mysql_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        query = "SELECT * FROM users_info WHERE screen_name = %s"
        cursor.execute(query, (username,))
        user_info = cursor.fetchone()
    except Exception as e:
        st.error(f"Error occurred while fetching user information from MySQL: {e}")
        return
    finally:
        cursor.close()
        conn.close()
    
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
        st.write(f"User Name: {user_name}")
        st.write(f"Screen Name: {user_screen_name}")
        st.write(f"Location: {user_location}")
        st.write(f"Description: {user_description}")
        if verified:
            st.write("Verified: Verified")
        else:
            st.write("Verified: Not Verified")
        st.write(f"Followers Count: {followers_count}")
        st.write(f"Friends Count: {friends_count}")
        st.write(f"Created At: {created_at}")
        st.write("---")

        st.write(f"Tweets by user with username '{user_screen_name}' (ID: {user_id}):")
        try:
            user_tweets = mongo_collection.find({"user_id": int(user_id)})
            tweet_count = 0
            for tweet in user_tweets:
                print(type(tweet), tweet) 
                tweet_count += 1
                tweet_text = tweet.get('text', 'No text available')
                st.write(f"Tweet {tweet_count}: '{tweet_text}'")
        except Exception as e:
            st.error(f"Error occurred while fetching user tweets from MongoDB: {e}")

    else:
        st.error(f"No user found with username '{username}' in MySQL.")


def display_tweets(tweets, page_number, tweets_per_page):
    mongo_client = mongo_db_connection()
    db = mongo_client.sample
    collection = db.tweets
    st.write(f"## Page {page_number}")
    start_index = (page_number - 1) * tweets_per_page
    end_index = min(page_number * tweets_per_page, len(tweets))
    for i in range(start_index, end_index):
        original_tweet = tweets[i]
        user_id = original_tweet['user_id']
        user_info = get_user_info(user_id)
        user_name = user_info.get('name', 'Unknown') if user_info else 'Unknown'
        
        # Display the original tweet's text as the title of the expander along with the user name
        original_tweet_text = original_tweet.get('text')
        expander_title = f"Tweet by {user_name}: {original_tweet_text}"
        
        # Display retweets within the expander
        with st.expander(expander_title):
            # Display retweets
            retweets = collection.find({"retweeted_status.id_str": original_tweet["id_str"]})
            retweet_count = 0
            for retweet in retweets:
                retweet_user_id = retweet['user_id']
                retweet_user_info = get_user_info(retweet_user_id)
                retweet_user_name = retweet_user_info.get('name', 'Unknown') if retweet_user_info else 'Unknown'
                
                retweet_count += 1
                retweet_text = retweet.get('text')
                st.info(f"  - **Retweet {retweet_count} by {retweet_user_name}:**\n{retweet_text}")


if __name__ == "__main__":
    main()
