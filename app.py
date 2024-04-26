import streamlit as st
import pymongo as pm
import os
import math
from datetime import datetime
import mysql.connector
from pymongo import MongoClient
from dotenv import load_dotenv
from pprint import pprint
from pymongo.mongo_client import MongoClient
import pymongo
from pymongo.server_api import ServerApi

mongo_client = None

def format_tweet_date(date_string):
    try:
        parsed_date = datetime.strptime(date_string, "%a %b %d %H:%M:%S %z %Y")
        formatted_date = parsed_date.strftime("%m/%d/%Y %I:%M %p")
        return formatted_date
    except ValueError:
        return "Invalid date format"
    
def format_tweet_datetime(date_string):
    try:
        parsed_date = datetime.strptime(date_string, "%m/%d/%Y")
        formatted_date = parsed_date.strftime("%m/%d/%Y %I:%M %p")
        return parsed_date
    except ValueError:
        return "Invalid date format"

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

def main():
    st.title("TWEET SEARCH ENGINE")

    # Connect to MongoDB
    mongo_client = mongo_db_connection()
    mongo_db = mongo_client.corona3
    mongo_collection = mongo_db.tweets

    # Get the current page from URL params
    current_page = st.experimental_get_query_params().get("page", ["search"])[0]

    # If on search page
    if current_page == "search":
        search_page()
    # If on results page
    elif current_page == "results":
        results_page(mongo_client)
    # If on user_info page
    elif current_page == "user_info":
        username = st.experimental_get_query_params().get("username", [None])[0]
        if username:
            user_info_page(username, mongo_collection)
        else:
            st.error("No username provided for user_info page.")


def search_page():
    st.subheader("Advanced Search Options")
    
    with st.form(key='search_form'):
        input_keyword = st.text_input("Enter Keyword (Optional)")
        input_hashtag = st.text_input("Enter Hashtag (Optional)")
        user_search = st.text_input("Enter username (Optional)")
        start_date = st.date_input("Start Date", datetime(2010, 1, 1))
        end_date = st.date_input("End Date", datetime.now())
        
        input_language = st.selectbox("Select Language", ["Select","en", "fr", "ge", "in"], index=0)
        form_submit_button = st.form_submit_button(label='Search')

    if form_submit_button:
        if user_search:
            if not (input_keyword or input_hashtag or input_language != "Select"):
                # Navigate to user info page
                st.experimental_set_query_params(page="user_info", username=user_search)
            else:
                # Set URL params for results page
                st.experimental_set_query_params(page="user_info", username=user_search, keyword=input_keyword, hashtag=input_hashtag, language=input_language)
        else:
            start_date = start_date.strftime("%m-%d-%Y")
            # Set URL params for results page
            end_date = end_date.strftime("%m-%d-%Y")
            st.experimental_set_query_params(page="results", keyword=input_keyword, hashtag=input_hashtag, language=input_language, start_date=start_date, end_date=end_date)
        st.experimental_rerun()


def results_page(mongo_client, keyword=None, hashtag=None, language="Select", start_date=None, end_date=None): 
    input_keyword = st.experimental_get_query_params().get("keyword", [""])[0]
    input_hashtag = st.experimental_get_query_params().get("hashtag", [""])[0]
    input_language = st.experimental_get_query_params().get("language", ["Select"])[0]
    input_start_date = st.experimental_get_query_params().get("start_date", [None])[0]
    input_end_date = st.experimental_get_query_params().get("end_date", [None])[0]
    db = mongo_client.sample_test
    collection = db.tweets_test

    query = {"retweeted_status": {"$exists": False}}
    if input_keyword:
        query["text"] = {"$regex": input_keyword, "$options": "i"}
    if input_hashtag:
        query["entities.hashtags.text"] = {"$regex": input_hashtag, "$options": "i"}
    if input_language != "Select":
        query["lang"] = input_language
    #if input_start_date and input_end_date:
    #    query["created_at"] = {"$gte": input_start_date, "$lte": input_end_date}

    try:
        original_tweets = collection.find(query).sort([("favorite_count", pymongo.DESCENDING), ("retweet_count", pymongo.DESCENDING), ("created_at", pymongo.DESCENDING)])

        top_tweets = []
        for original_tweet in original_tweets:
            top_tweets.append(original_tweet)
            if len(top_tweets) == 50:
                break

        num_pages = math.ceil(len(top_tweets) / 10)

        page_number = st.number_input("Page Number", min_value=1, max_value=num_pages, value=1, key="page_number")
        tweets_per_page = 10

        # Display top users and top tweets with multiple metrics on sidebar
        st.sidebar.title("Top Users and Tweets")
        metric = st.sidebar.selectbox("Select Metric", ["Top Users", "Top Tweets"])

        conn = mysql_db_connection()
        cursor = conn.cursor(dictionary=True)
        try:
            if metric == "Top Users":
                # Fetch user information including names and followers count
                query = "SELECT screen_name, name, followers_count FROM users_info ORDER BY followers_count DESC LIMIT 5"
                cursor.execute(query)
                top_users = cursor.fetchall()
                # Display top users on sidebar
                st.sidebar.subheader("Top Users by Followers Count")
                st.sidebar.write("---")
                for user in top_users:
                    user_name = user.get('name', 'Unknown')
                    user_screen_name = user.get('screen_name', 'Unknown')
                    followers_count = user.get('followers_count', 0)
                    # Add hyperlinks to the username
                    user_link = f"[{user_name} (@{user_screen_name})](?page=user_info&username={user_screen_name})"
                    st.sidebar.markdown(f"**{user_link}**")
                    st.sidebar.write(f"Followers: {followers_count}")
                    st.sidebar.write("---")
            elif metric == "Top Tweets":
                # Fetch top tweets with highest favorite count
                query_side = {"retweeted_status": {"$exists": False}}
                top_tweets_query = collection.find(query_side).sort("favorite_count", pymongo.DESCENDING).limit(5)
                top_tweets_side = list(top_tweets_query)
                # Display top tweets on sidebar
                st.sidebar.subheader("Top Tweets by Favorite Count")
                for tweet in top_tweets_side:
                    user_id_side = tweet.get('user_id', 'Unknown')
                    user_info_side = get_user_info(user_id_side)
                    user_name_side = user_info_side.get('screen_name', 'Unknown') if user_info_side else 'Unknown'
                    user_link_side = f"[@{user_name_side}](?page=user_info&username={user_name_side})"
                    tweet_text_side = tweet.get('text', 'Unknown')
                    favorite_count_side = tweet.get('favorite_count', 0)
                    st.sidebar.write(f"User: @{user_link_side}")
                    st.sidebar.write(f"Tweet: {tweet_text_side}")
                    st.sidebar.write(f"Favorite Count: {favorite_count_side}")
                    st.sidebar.write("---")
        except Exception as e:
            st.error(f"Error occurred while fetching top users and tweets: {e}")
        finally:
            cursor.close()
            conn.close()

        display_tweets(mongo_client, top_tweets, page_number, tweets_per_page)
    except Exception as e:
        st.error(f"Error occurred while searching MongoDB: {e}")



def user_info_page(username, mongo_collection, keyword=None, hashtag=None, language="Select"):
    # Connect to MySQL and fetch user information
    input_keyword = st.experimental_get_query_params().get("keyword", [""])[0]
    input_hashtag = st.experimental_get_query_params().get("hashtag", [""])[0]
    input_language = st.experimental_get_query_params().get("language", ["Select"])[0]
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
        profile_pic_url = "https://abs.twimg.com/sticky/default_profile_images/default_profile_normal.png"
        col1, col2 = st.columns([4, 1])
        with col1:
            st.write(f"### {user_name} (@{user_screen_name}) {'✓' if verified else ''}")
            st.write(f"**Location:** {user_location}")
            st.write(f"**Description:** {user_description}")
            st.write(f"**Followers Count:** {followers_count}")
            st.write(f"**Friends Count:** {friends_count}")
            st.write(f"**Created At:** {created_at}")
            st.write("---")
        with col2:
            st.image(profile_pic_url, width=100)

        
        st.write("### TWEETS")
        try:
            # Define query criteria for keyword and hashtag
            query_criteria = {"user_id": user_id}
            if input_keyword:
                query_criteria["$or"] = [{"text": {"$regex": input_keyword, "$options": "i"}}]
            if input_hashtag:
                query_criteria["$or"] = [{"entities.hashtags.text": {"$regex": input_hashtag, "$options": "i"}}]
            if input_language != "Select":
                query_criteria["lang"] = input_language
            # Find top 50 tweets based on favorite count
            user_tweets = mongo_collection.find(query_criteria)
            tweets = list(user_tweets)
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
                    user_info = get_user_info(retweet.get('user_id'))
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




def display_tweets(mongo_client, tweets, page_number, tweets_per_page):
    st.write(f"## Page {page_number}")
    start_index = (page_number - 1) * tweets_per_page
    end_index = min(page_number * tweets_per_page, len(tweets))
    for i in range(start_index, end_index):
        original_tweet = tweets[i]
        user_id = original_tweet['user_id']
        user_info = get_user_info(user_id)
        user_name = user_info.get('name', 'Unknown') if user_info else 'Unknown'
        user_screen_name = user_info.get('screen_name', 'Unknown') if user_info else 'Unknown'
        user_display = f"{user_name} ([@{user_screen_name}](?page=user_info&username={user_screen_name}))"

        # Display the original tweet's text along with the user name
        original_tweet_text = original_tweet.get('text')
        created_at = original_tweet.get('created_at', 'Unknown')
        created_at_formatted = format_tweet_date(created_at)
        st.write("---")
        st.write(f"### Tweet by {user_display}")
        st.write(original_tweet_text)
        st.write(f"📅: {created_at_formatted}")
        # Display quoted status if available
        quoted_status = original_tweet.get("quoted_status")
        if quoted_status:
            quoted_user_info = get_user_info(quoted_status['user_id'])
            quoted_user_name = quoted_user_info.get('name', 'Unknown') if quoted_user_info else 'Unknown'
            quoted_user_screen_name = quoted_user_info.get('screen_name', 'Unknown') if quoted_user_info else 'Unknown'
            quoted_user_display = f"{quoted_user_name} ([@{quoted_user_screen_name}](?page=user_info&username={quoted_user_screen_name}))"
            quoted_tweet_text = quoted_status.get('text', 'No text available')
            st.write(f">>> Quoted Tweet by {quoted_user_display}: {quoted_tweet_text}")
        
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
                user_info = get_user_info(retweet.get('user_id'))
                if user_info:
                    user_name = user_info.get('name', 'Unknown')
                    user_screen_name = user_info.get('screen_name', 'Unknown')
                    user_display = f"{user_name} ([@{user_screen_name}](?page=user_info&username={user_screen_name}))"
                    retweet_user_names.append(user_display)
            if retweet_user_names:
                st.info(f"Retweeted by: {', '.join(retweet_user_names)}")

if __name__ == "__main__":
    main()