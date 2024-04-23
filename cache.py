from pymongo import MongoClient
from collections import OrderedDict
import time, os
# from app import mongo_db_connection
from dotenv import load_dotenv
load_dotenv()
from pymongo.server_api import ServerApi
import mysql.connector


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


class TwitterSearchApp:
    def __init__(self, max_cache_size=100):
        self.max_cache_size = max_cache_size
        self.cache = {}
        self.cache['user'] = OrderedDict()      # OrderedDict to implement LRU Cache
        self.cache['tweet'] = OrderedDict()
        self.cache['hashtag'] = OrderedDict()
        self.cache_ttl = 2                     # TTL of 60 seconds for each cache entry
        # self.client = MongoClient('mongodb://localhost:27017/')
        self.client = mongo_db_connection()
        print("client",self.client)
        self.cache_collection = self.client['twitter_db']['cache']
        self.db = self.client.sample
        self.collection = self.db.tweets
        self.conn = mysql_db_connection()


    def load_cache_from_mongodb(self):
        cache_data = self.cache_collection.find_one()
        if cache_data:
            self.cache['user'] = OrderedDict(cache_data['cache']['user'])
            self.cache['tweet'] = OrderedDict(cache_data['cache']['tweet'])
            self.cache['hashtag'] = OrderedDict(cache_data['cache']['hashtag'])


    def search_cache(self, entity, keyword):    # entity = "user" / "tweet" / "hashtag" 
        cache = self.cache[entity]
        if keyword in cache:                    # Result found in cache
            print("Result found in cache")
            result = cache[keyword]['result']
            timestamp = cache[keyword]['timestamp']
            if time.time() - cache[keyword]['timestamp'] < self.cache_ttl:    # Cache entry still valid
                cache.move_to_end(keyword)                  # Mark entry as recently used
                return result
            else:                                           # Cache entry expired, delete the key
                del cache[keyword]

        print("Searching in MongoDB:")
        if entity == 'tweet':
            result = self.query_mongodb_tweet(keyword)
        elif entity == 'user':
            result = self.query_mongodb_user(keyword)
        elif entity == 'hashtag':
            result = self.query_mongodb_hashtag(keyword)
        if len(cache) >= self.max_cache_size:     # No space in the cache, delete the recently used entry
            cache.popitem(last=False)
        cache[keyword] = {
            "result": result,
            "timestamp": time.time()
        }
        # self.cache_collection.update_one({}, {'$set': {'cache': self.cache}}, upsert=True)
        return result


    def search_tweet(self, keyword):
        return self.search_cache('tweet', keyword)
    

    def search_user(self, keyword):
        return self.search_cache('user', keyword)


    def search_hashtag(self, keyword):
        if not keyword.startswith('#'):
            keyword = f"#{keyword}"
        return self.search_cache('hashtag', keyword)


    def query_mongodb_tweet(self, keyword):
        # Placeholder for MongoDB query, replace with actual query to search for tweets
        # return f"Dummy result for Tweets: {keyword}"
        query = {"retweeted_status": {"$exists": False}}
        query["text"] = {"$regex": keyword, "$options": "i"}
        original_tweets = self.collection.find(query).sort([("retweet_count", -1), ("favorite_count", -1)])
        print("original_tweets",original_tweets)

        top_tweets = []
        for original_tweet in original_tweets:
            retweet_exists = self.collection.find_one({"retweeted_status.id_str": original_tweet["id_str"]})
            if retweet_exists:
                top_tweets.append(original_tweet)
                if len(top_tweets) == 50:
                    break
        print("top_tweets",len(top_tweets))
        return top_tweets


    def query_mongodb_user(self, keyword):
        # Placeholder for MongoDB query, replace with actual query to search for users
        # return f"Dummy result for User: {keyword}"
        cursor = self.conn.cursor(dictionary=True)
        query = "SELECT * FROM users_info WHERE screen_name = %s"
        cursor.execute(query, (keyword,))
        user_info = cursor.fetchone()
        conn.close()
        return user_info


    def query_mongodb_hashtag(self, keyword):
        # Placeholder for MongoDB query, replace with actual query to search for hashtags
        # return f"Dummy result for Hashtag: {keyword}"
        query = {"retweeted_status": {"$exists": False}}
        query["entities.hashtags.text"] = {"$regex": keyword, "$options": "i"}
        original_tweets = self.collection.find(query).sort([("retweet_count", -1), ("favorite_count", -1)])
        print("original_tweets",original_tweets)

        top_tweets = []
        for original_tweet in original_tweets:
            retweet_exists = self.collection.find_one({"retweeted_status.id_str": original_tweet["id_str"]})
            if retweet_exists:
                top_tweets.append(original_tweet)
                if len(top_tweets) == 50:
                    break
        print("top_tweets",len(top_tweets))
        return top_tweets

    def shutdown(self):
        print("Shutting down", self.cache)
        self.cache_collection.update_one({}, {'$set': {'cache': self.cache}}, upsert=True)
        self.client.close()

# Example usage
if __name__ == "__main__":
    app = TwitterSearchApp(max_cache_size=10)
    app.load_cache_from_mongodb()

    # Example searches
    # print(app.search_tweet("prisoners"))
    # print(app.search_tweet("data science1"))
    # print(app.search_user("udit"))
    print(app.search_hashtag("#Corona"))

    app.shutdown()