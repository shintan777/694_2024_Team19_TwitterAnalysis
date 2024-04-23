import streamlit as st
import pymongo as pm
import os
import math
import mysql.connector
from pymongo import MongoClient
from dotenv import load_dotenv
from pprint import pprint
from pymongo.mongo_client import MongoClient
import pymongo
from pymongo.server_api import ServerApi

mongo_client = None

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
    st.title("Tweet Search Engine")

    # Connect to MongoDB
    mongo_client = mongo_db_connection()
    mongo_db = mongo_client.sample
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
        input_language = st.selectbox("Select Language", ["Select","en", "fr", "ge", "in"], index=0)
        
        # User search input
        user_search = st.text_input("Search for a user by username")

        form_submit_button = st.form_submit_button(label='Search')

    if form_submit_button:
        if user_search and (input_keyword or input_hashtag or input_language != "Select"):
            # Navigate to user info page
            st.experimental_set_query_params(page="user_info", username=user_search, keyword=input_keyword, hashtag=input_hashtag, language=input_language)
        else:
            # Set URL params for results page
            st.experimental_set_query_params(page="results", keyword=input_keyword, hashtag=input_hashtag, language=input_language)
        st.experimental_rerun()


def results_page(mongo_client): #Queries for search by keyword, hashtag, and language
    # Get search parameters from URL params
    input_keyword = st.experimental_get_query_params().get("keyword", [""])[0]
    input_hashtag = st.experimental_get_query_params().get("hashtag", [""])[0]
    input_language = st.experimental_get_query_params().get("language", ["en"])[0]

    # Fetch results based on search parameters
    db = mongo_client.sample
    collection = db.tweets

    # Define the MongoDB query based on search parameters
    query = {"retweeted_status": {"$exists": False}}
    if input_keyword:
        query["text"] = {"$regex": input_keyword, "$options": "i"}
    if input_hashtag:
        query["entities.hashtags.text"] = {"$regex": input_hashtag, "$options": "i"}
    if input_language != "Select":
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
        display_tweets(mongo_client, top_tweets, page_number, tweets_per_page)
    except Exception as e:
        st.error(f"Error occurred while searching MongoDB: {e}")

def user_info_page(username, mongo_collection, keyword=None, hashtag=None):
    # Connect to MySQL and fetch user information
    input_keyword = st.experimental_get_query_params().get("keyword", [""])[0]
    input_hashtag = st.experimental_get_query_params().get("hashtag", [""])[0]
    input_language = st.experimental_get_query_params().get("language", ["en"])[0]
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

        st.write(f"TWEETS BY '{user_screen_name}' (ID: {user_id}) based on favorite count:")
        try:
            # Define query criteria for keyword and hashtag
            query_criteria = {"user_id": int(user_id)}
            if input_keyword:
                query_criteria["text"] = {"$regex": input_keyword, "$options": "i"}
            if input_hashtag:
                query_criteria["entities.hashtags.text"] = {"$regex": input_hashtag, "$options": "i"}
            if input_language != "Select":
                query_criteria["lang"] = input_language

            # Find top 50 tweets based on favorite count
            user_tweets = mongo_collection.find(query_criteria).sort("favorite_count", pymongo.DESCENDING).limit(50)

            # Display tweets
            for tweet in user_tweets:
                st.write("---")
                st.write(f"Tweet ID: {tweet['_id']}")
                st.write(f"Text: {tweet['text']}")
                st.write(f"Favorite Count: {tweet['favorite_count']}")
                st.write(f"Retweet Count: {tweet['retweet_count']}")
                st.write(f"Language: {tweet['lang']}")
        except Exception as e:
            st.error(f"Error occurred while fetching user tweets from MongoDB: {e}")
    else:
        st.error(f"No user found with username '{username}' in MySQL.")


def display_tweets(mongo_client, tweets, page_number, tweets_per_page):
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
        user_screen_name = user_info.get('screen_name', 'Unknown') if user_info else 'Unknown'
        user_display = f"{user_name} ({user_screen_name})"
        
        # Display the original tweet's text as the title of the expander along with the user name
        original_tweet_text = original_tweet.get('text')
        expander_title = f"Tweet by {user_display}: {original_tweet_text}"
        
        # Display quoted status if available
        quoted_status = original_tweet.get("quoted_status")
        if quoted_status:
            quoted_user_info = get_user_info(quoted_status['user_id'])
            quoted_user_name = quoted_user_info.get('name', 'Unknown') if quoted_user_info else 'Unknown'
            quoted_user_screen_name = quoted_user_info.get('screen_name', 'Unknown') if quoted_user_info else 'Unknown'
            quoted_user_display = f"{quoted_user_name} ({quoted_user_screen_name})"
            quoted_tweet_text = quoted_status.get('text', 'No text available')
        
        # Display retweets within the expander
        with st.expander(expander_title):
            # Display retweets
            retweets = collection.find({"retweeted_status.id_str": original_tweet["id_str"]})
            retweet_count = 0
            retweet_user_names = []
            for retweet in retweets:
                retweet_text = retweet.get('text')
                if retweet_text == original_tweet_text:
                    retweet_user_id = retweet['user_id']
                    retweet_user_info = get_user_info(retweet_user_id)
                    retweet_user_name = retweet_user_info.get('name', 'Unknown') if retweet_user_info else 'Unknown'
                    retweet_user_screen_name = retweet_user_info.get('screen_name', 'Unknown') if retweet_user_info else 'Unknown'
                    retweet_user_display = f"{retweet_user_name} ({retweet_user_screen_name})"
                    retweet_user_names.append(retweet_user_display)
                    retweet_count += 1
            if retweet_count == 0:
                st.info("No retweets.")
            else:
                if retweet_user_names:
                    st.info(f"Retweeted by: {', '.join(retweet_user_names)}")
                else:
                    st.info("No retweets with matching text.")
            # Display quoted tweet
            if quoted_status:
                st.info(f"Quoted Tweet by {quoted_user_display}: {quoted_tweet_text}")


if __name__ == "__main__":
    main()
