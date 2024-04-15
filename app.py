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
        password = "password"
    )
    print("Connected to MySQL!")
    return conn

def main():
    st.title("Tweet Search Engine")

    # Get the current page from URL params
    current_page = st.experimental_get_query_params().get("page", ["search"])[0]

    # If on search page
    if current_page == "search":
        search_page()
    # If on results page
    elif current_page == "results":
        results_page()


def search_page():
    st.subheader("Advanced Search Options")
    input_keyword = st.text_input("Enter Keyword (Optional)")
    input_hashtag = st.text_input("Enter Hashtag (Optional)")
    input_language = st.selectbox("Select Language", ["en", "fr", "ge", "in", "Other"], index=0)
    
    # When search button is clicked, navigate to results page with search parameters
    if st.button("Search"):
        # Set URL params for results page
        st.experimental_set_query_params(page="results", keyword=input_keyword, hashtag=input_hashtag, language=input_language)


def results_page():
    # Get search parameters from URL params
    input_keyword = st.experimental_get_query_params().get("keyword", [""])[0]
    input_hashtag = st.experimental_get_query_params().get("hashtag", [""])[0]
    input_language = st.experimental_get_query_params().get("language", ["en"])[0]

    # Connect to MongoDB and fetch results based on search parameters
    mongo_client = mongo_db_connection()
    db = mongo_client.sample
    collection = db.tweets

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


def display_tweets(tweets, page_number, tweets_per_page):
    mongo_client = mongo_db_connection()
    db = mongo_client.sample
    collection = db.tweets
    st.write(f"## Page {page_number}")
    start_index = (page_number - 1) * tweets_per_page
    end_index = min(page_number * tweets_per_page, len(tweets))
    for i in range(start_index, end_index):
        original_tweet = tweets[i]
        with st.expander(original_tweet['text']):
            retweets = collection.find({"retweeted_status.id_str": original_tweet["id_str"]})
            retweet_count = 0
            for retweet in retweets:
                retweet_count += 1
                st.info(f"  - **Retweet {retweet_count}:**\n{retweet['text']}")


if __name__ == "__main__":
    main()
