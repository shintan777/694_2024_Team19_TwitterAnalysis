import streamlit as st
import pymongo as pm
import os
import math
import pymongo
import mysql.connector
import time
import atexit

from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv
from pprint import pprint
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

from cache import app
mongo_client = None


def shutdown():
    global app
    print("Shutting down")
    #app.shutdown()


# +
#atexit.register(shutdown)
# -

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

def main():
    st.title("TWEET SEARCH ENGINE")

    # Get the current page from URL params
    current_page = st.experimental_get_query_params().get("page", ["search"])[0]

    # If on search page
    if current_page == "search":
        search_page()
    # If on results page
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
        user_search = st.text_input("Enter username (Optional)")
        start_date = st.date_input("Start Date", datetime(2010, 1, 1))
        end_date = st.date_input("End Date", datetime.now())
        
        input_language = st.selectbox("Select Language", ["Select","en", "fr", "ge", "in"], index=0)
        form_submit_button = st.form_submit_button(label='Search')

    if form_submit_button:
        if user_search:
            if not (input_keyword or input_hashtag or input_language != "Select"):
                st.experimental_set_query_params(page="user_info", username=user_search)
            else:
                st.experimental_set_query_params(page="user_info", username=user_search, keyword=input_keyword, hashtag=input_hashtag, language=input_language)
        else:
            start_date = start_date.strftime("%m-%d-%Y")
            end_date = end_date.strftime("%m-%d-%Y")
            st.experimental_set_query_params(page="results", keyword=input_keyword, hashtag=input_hashtag, language=input_language, start_date=start_date, end_date=end_date)
        st.experimental_rerun()


# +
def results_page(keyword=None, hashtag=None, language="Select", start_date=None, end_date=None): 
    global app
    
    start_time = time.time()
    input_keyword = st.experimental_get_query_params().get("keyword", [""])[0]
    input_hashtag = st.experimental_get_query_params().get("hashtag", [""])[0]
    input_language = st.experimental_get_query_params().get("language", ["Select"])[0]
    input_start_date = st.experimental_get_query_params().get("start_date", [None])[0]
    input_end_date = st.experimental_get_query_params().get("end_date", [None])[0]
    # db = mongo_client.sample_test
    # collection = db.tweets_test

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
        original_tweets = app.collection.find(query).sort([("favorite_count", pymongo.DESCENDING), ("retweet_count", pymongo.DESCENDING), ("created_at", pymongo.DESCENDING)])

        top_tweets = []
        if input_keyword:
            top_tweets += app.search_cache(entity = "tweet", keyword = input_keyword, lang = input_language)
        if input_hashtag:
            top_tweets += app.search_cache(entity = "hashtag", keyword = input_keyword, lang = input_language)

        num_pages = math.ceil(len(top_tweets) / 10)
        page_number = st.number_input("Page Number", min_value=1, max_value=num_pages, value=1, key="page_number")
        tweets_per_page = 10

        st.sidebar.title("Top Users and Tweets")
        metric = st.sidebar.selectbox("Select Metric", ["Top Users", "Top Tweets"])


        # Perform an action when the value changes
        # Define your options (list of strings)
#         options = ['Option 1', 'Option 2', 'Option 3']

#         # Create the select box
#         selected_option = st.selectbox('Select an option', options)

#         if st.session_state.selected_option != selected_option:
#             st.session_state.selected_option = selected_option
#             # Your custom action goes here
#             st.write(f"Selected option changed to: {selected_option}")

        cursor = app.conn.cursor(dictionary=True)
        try:
            if metric == "Top Users":
                query = "SELECT screen_name, name, followers_count FROM users_info ORDER BY followers_count DESC LIMIT 5"
                cursor.execute(query)
                top_users = cursor.fetchall()
                st.sidebar.subheader("Top Users by Followers Count")
                st.sidebar.write("---")
                for user in top_users:
                    user_name = user.get('name', 'Unknown')
                    user_screen_name = user.get('screen_name', 'Unknown')
                    followers_count = user.get('followers_count', 0)
                    user_link = f"[{user_name} (@{user_screen_name})](?page=user_info&username={user_screen_name})"
                    st.sidebar.markdown(f"**{user_link}**")
                    st.sidebar.write(f"Followers: {followers_count}")
                    st.sidebar.write("---")
            elif metric == "Top Tweets":
                # Fetch top tweets with highest favorite count
                query_side = {"retweeted_status": {"$exists": False}}
                top_tweets_query = app.collection.find(query_side).sort("favorite_count", pymongo.DESCENDING).limit(5)
                top_tweets_side = list(top_tweets_query)
                # Display top tweets on sidebar
                st.sidebar.subheader("Top Tweets by Favorite Count")
                for tweet in top_tweets_side:
                    user_id_side = tweet.get('user_id', 'Unknown')
                    user_info_side = app.search_cache("user", user_deets = [user_id_side, None])
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
  
        display_tweets(top_tweets, page_number, tweets_per_page, start_time)
    except Exception as e:
        st.error(f"Error occurred while searching MongoDB: {e}")

# -

def display_tweets(tweets, page_number, tweets_per_page, start_time):
    global app
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