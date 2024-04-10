import streamlit as st
import pymongo as pm
import json
import os
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
    
    # Connect to MongoDB and MySQL
    try:
        mongo_client = mongo_db_connection()
        mysql_client = mysql_db_connection()
        db = mongo_client.sample
        collection = db.tweets
        st.success("Connected to databases successfully!")
    except Exception as e:
        st.error(f"Failed to connect to databases: {e}")
        return
    
    # Get the input keyword
    input_keyword = st.text_input("Enter the keyword to search")
    
    # Search keyword in MongoDB and display results 
    if st.button("Search in MongoDB"):
        try:
            keyword = input_keyword
            query = {"text": {"$regex": keyword, "$options": "i"}}
            matching_tweets = collection.find(query)

            count = 0
            for tweet in matching_tweets:
                count += 1
                st.text_area(f"Tweet {count}", value=tweet['text'], height=100)

            st.write("{} tweets found".format(count))
        except Exception as e:
            st.error(f"Error occurred while searching MongoDB: {e}")

if __name__ == "__main__":
    main()
