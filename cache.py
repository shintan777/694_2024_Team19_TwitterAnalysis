from pymongo import MongoClient
from collections import OrderedDict
import time, os
# from app import mongo_db_connection
from dotenv import load_dotenv
load_dotenv()
from pymongo.server_api import ServerApi
import mysql.connector
import schedule
import asyncio

# For static cache implementation
import nltk
nltk.download('corpora')
from nltk.corpus import stopwords
from collections import Counter

def mongo_db_connection():
    mongo_client = None
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

# +
class TwitterSearchApp:
    def __init__(self, max_cache_size=100):
        self.max_cache_size = max_cache_size
        self.cache = {}
        self.cache['user'] = OrderedDict() 
        self.cache['tweet'] = OrderedDict()
        self.cache['hashtag'] = OrderedDict()
        self.cache_ttl = float('inf') 
        self.client = mongo_db_connection()
        self.cache_collection = self.client['twitter_db']['cache_test']
        self.db = self.client.sample_test
        self.collection = self.db.tweets_test
        self.conn = mysql_db_connection()

    def load_cache_from_mongodb(self):
        cache_data = self.cache_collection.find_one()
        if cache_data:
            self.cache['user'] = OrderedDict(cache_data['cache']['user'])
            self.cache['tweet'] = OrderedDict(cache_data['cache']['tweet'])
            self.cache['hashtag'] = OrderedDict(cache_data['cache']['hashtag'])

    def search_cache(self, entity, keyword = None, user_deets = [], lang="Select"):    # entity = "user" / "tweet" / "hashtag" 

        cache = self.cache[entity]
        if user_deets:
            uid, uname = user_deets[0], user_deets[1]
            if uid:
                keyword = uid
            elif uname:
                keyword = uname
            else:
                print("Incorrect User deets")
            
        if keyword in cache:                    
            print("Result found in cache")
            result = cache[keyword]['result']
            timestamp = cache[keyword]['timestamp']
            if time.time() - cache[keyword]['timestamp'] < self.cache_ttl:    # Cache entry still valid
                cache.move_to_end(keyword)                  # Mark entry as recently used
                return result
            else:                                           # Cache entry expired, delete the key
                del cache[keyword]
                
        input_keyword, input_user, input_hashtag = None, None, None
        
        if entity == 'tweet':
            input_keyword = keyword
        elif entity == 'user':
            input_user = keyword
        elif entity == 'hashtag':
            input_hashtag = keyword
            
        if input_keyword or input_hashtag:
            result = self.query_mongodb_tweet(input_keyword, input_hashtag, lang)
            
        elif input_user:
            if uid:
                result = self.query_sql_user(uid)
            elif uname:
                result = self.query_sql_user_info(uname)
            else:
                print("Incorrect User deets")
            
            if result:
                print("user found in sql db")
            else:
                print("no user found")
        else:
            print("Incorrect params")

        if len(cache) >= self.max_cache_size:     # No space in the cache, delete the recently used entry
            cache.popitem(last=False)
        cache[keyword] = {
            "result": result,
            "timestamp": time.time()
        }
        # self.cache_collection.update_one({}, {'$set': {'cache': self.cache}}, upsert=True)
        return result

#     def search_tweet(self, keyword):
#         return self.search_cache('tweet', keyword)

#     def search_user(self, keyword):
#         return self.search_cache('user', keyword)


#     def search_hashtag(self, keyword):
#         if not keyword.startswith('#'):
#             keyword = f"#{keyword}"
#         return self.search_cache('hashtag', keyword)

    def query_mongodb_tweet(self, input_keyword=None, input_hashtag=None, input_language="Select"):
        query = {}
        if input_keyword:
            query["text"] = {"$regex": input_keyword, "$options": "i"}
        if input_hashtag:
            query["entities.hashtags.text"] = {"$regex": input_hashtag, "$options": "i"}
        if input_language != "Select":
            query["lang"] = input_language

        original_tweets = self.collection.find(query).sort([("retweet_count", -1), ("favorite_count", -1)])
 
        top_tweets = []
        for original_tweet in original_tweets:
            top_tweets.append(original_tweet)
            if len(top_tweets) == 50:
                break

        print("top_tweets",len(top_tweets))
        return top_tweets

    def query_sql_user(self, uid):
        cursor = self.conn.cursor(dictionary=True)
        query = "SELECT * FROM users_info WHERE id = %s"
        cursor.execute(query, (uid,))
        user_info = cursor.fetchone()
        cursor.close()
        return user_info
    
    def query_sql_user_info(self, username):
        cursor = self.conn.cursor(dictionary=True)
        query = "SELECT * FROM users_info WHERE screen_name = %s"
        cursor.execute(query, (username,))
        user_info = cursor.fetchone()
        cursor.close()
        return user_info

    def tweets_for_users(self, user_id, input_keyword, input_hashtag, input_language):
        query_criteria = {"user_id": user_id}
        if input_keyword:
            query_criteria["$or"] = [{"text": {"$regex": input_keyword, "$options": "i"}}]
        if input_hashtag:
            query_criteria["$or"] = [{"entities.hashtags.text": {"$regex": input_hashtag, "$options": "i"}}]
        if input_language != "Select":
            query_criteria["lang"] = input_language
            # Find top 50 tweets based on favorite count
        user_tweets = self.collection.find(query_criteria).sort([("retweet_count", -1), ("favorite_count", -1)])
        return user_tweets
    
#     def query_mongodb_hashtag(self, keyword):
#         # Placeholder for MongoDB query, replace with actual query to search for hashtags
#         # return f"Dummy result for Hashtag: {keyword}"
#         query = {"retweeted_status": {"$exists": False}}
#         query["entities.hashtags.text"] = {"$regex": keyword, "$options": "i"}
#         original_tweets = self.collection.find(query).sort([("retweet_count", -1), ("favorite_count", -1)])
#         print("original_tweets from mongo",original_tweets)

#         top_tweets = []
#         for original_tweet in original_tweets:
#             retweet_exists = self.collection.find_one({"retweeted_status.id_str": original_tweet["id_str"]})
#             if retweet_exists:
#                 top_tweets.append(original_tweet)
#                 if len(top_tweets) == 50:
#                     break
#         print("top_tweets",len(top_tweets))
#         return top_tweets

    
    async def checkpoint(self):
        print("Checkpointing")
        self.cache_collection.update_one({}, {'$set': {'cache': self.cache}}, upsert=True)

    def cache_top_10_keywords(self):
        # parse through each tweet and maintain a dict of top 10 keywords
        # Retrieve all tweets
        print("Started caching top keywords in tweets: " + time.time())
        tweets = self.collection.find({}, {"text": 1})

        # Define stopwords - words to ignore
        stop_words = set(stopwords.words('english'))

        # Count occurrences of each keyword in all tweets
        keyword_count = Counter()
        for tweet in tweets:
            text = tweet["text"]
            keywords = extract_keywords(text, stop_words)
            keyword_count.update(keywords)

        # Get top 10 keywords and store in cache
        top_keywords = keyword_count.most_common(10)
        for keyword in top_keywords:
            self.search_cache(keyword)
        print("Completed caching top keywords in tweets: " + time.time())

    def cron_cache_top_keywords(self):
        print("Cron for caching has started at: " +  + time.time())
        self.cache_top_10_keywords(self)
        print("Cron for caching is completed at: " +  + time.time())
        
    def shutdown(self):
        print("Shutting down", self.cache)
        self.cache_collection.update_one({}, {'$set': {'cache': self.cache}}, upsert=True)
        self.client.close()


async def main():
    app = TwitterSearchApp(max_cache_size=10)
    app.load_cache_from_mongodb()
    app.cache_top_10_keywords()
    print(app.cache["tweet"].keys())
    
    # create a cron to cache top 10 keywords daily at midnight
    schedule.every().day.at("00:00").do(app.cache_top_10_keywords)
    while True:
        await asyncio.sleep(300)    # Checkpoint every 5 minutes
        await app.checkpoint()
        schedule.run_pending()

if __name__ == "__main__":
    asyncio.run(main())

# +
# Example usage
# if __name__ == "__main__":
#     app = TwitterSearchApp(max_cache_size=10)
#     app.load_cache_from_mongodb()

#     # Example searches
#     print(app.search_tweet("prisoners"))
#     # print(app.search_tweet("data science1"))
#     # print(app.search_user("udit"))
# #     print(app.search_hashtag("#Corona"))

#     app.shutdown()

# +
# app = TwitterSearchApp(max_cache_size=2)
# app.load_cache_from_mongodb()
# print(app.cache)

# +
# app.search_cache("tweet", "weirdo")

# +
# print(app.cache)

# +
# app.search_cache("tweet", "weirdo")

# +
# app.search_cache("tweet", "death")

# +
# print(app.cache["tweet"].keys())

# +
# app.search_cache("tweet", "corona")

# +
# print(app.cache["tweet"].keys())

# +
# app.search_cache("tweet", "corona")

# +
# print(app.cache["tweet"].keys())

# +
# app.shutdown()

# +
# app = TwitterSearchApp(max_cache_size=2)
# app.load_cache_from_mongodb()
# print(app.cache)
