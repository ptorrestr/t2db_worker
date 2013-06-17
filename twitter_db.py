#! /usr/bin/python

import sqlite3
from pymongo import MongoClient
import api
import json
from search import Search

def boolToInt(bool_):
    if (bool_):
        return 1
    else:
        return 0

class sqlite_db(object):

    def __init__(self, db_file, 
                    db_tweets_table, 
                    db_searches_table ):
        self.db_file = db_file
        self.db_tweets_table = db_tweets_table
        self.db_searches_table = db_searches_table

    def connect(self):
        self.conn = sqlite3.connect( self.db_file, isolation_level=None)
        self.cur = self.conn.cursor()
        self.create_tweets_table()
        self.create_searches_table()
        self.create_tweets_searches_nav()

    def close(self):
        self.conn.close()

    def create_tweets_table(self):
        self.cur.execute("""CREATE TABLE IF NOT EXISTS """
            +self.db_tweets_table+"""(
            created_at TEXT,
            eventindex INT,
            favorited INT,
            id INT PRIMARY KEY,
            text TEXT,"""
           #location TEXT,
         """in_reply_to_screen_name TEXT,
            in_reply_to_user_id TEXT,
            in_reply_to_status_id TEXT,
            truncated INT,
            source TEXT,
            urls TEXT,
            user_mentions TEXT,
            hashtags TEXT,
            geo TEXT,
            place TEXT,
            coordinates TEXT,
            contributors TEXT,
            retweeted INT,"""
           #retweeted_status TEXT,
         """retweet_count INT,
            user_id INT,
            user_name TEXT,
            user_screen_name TEXT,
            user_location TEXT,
            user_description TEXT,
            user_profile_image_url TEXT,
            user_profile_background_tile INT,
            user_profile_background_image_url TEXT,
            user_profile_sidebar_fill_color TEXT,
            user_profile_background_color TEXT,
            user_profile_link_color TEXT,
            user_profile_text_color TEXT,
            user_protected INT,
            user_utc_offset INT,
            user_time_zone TEXT,
            user_followers_count INT,
            user_friends_count INT,
            user_statuses_count INT,
            user_favourites_count INT,
            user_url TEXT,"""
            #user_status TEXT,
       """  user_geo_enabled INT,
            user_verified INT,
            user_lang TEXT,
            user_notifications INT,
            user_contributors_enabled INT,
            user_created_at TEXT,
            user_listed_count INT
            )""")

    def create_searches_table(self):
        self.cur.execute("""CREATE TABLE IF NOT EXISTS """
            + self.db_searches_table + """(
            id INT PRIMARY KEY,
            created_at TEXT,
            query TEXT
            )""")

    def create_tweets_searches_nav(self):
        nav_name = self.db_tweets_table + "_" + self.db_searches_table
        self.cur.execute("""CREATE TABLE IF NOT EXISTS """
            + nav_name + """(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tweet_id INTEGER,
            search_id INTEGER,
            FOREIGN KEY(tweet_id) REFERENCES """ + self.db_tweets_table
            + """ (id),
            FOREIGN KEY(search_id) REFERENCES """ + self.db_searches_table
            + """ (id)
            )""")

    def insert_tweet(self, tweet, search, eventindex = 0):
        if not self.exist_tweet(tweet.id):
            self.cur.execute('INSERT INTO '+self.db_tweets_table+' VALUES('
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?) ',
                (tweet.created_at,
                eventindex,
                boolToInt(tweet.favorited),
                tweet.id,
                tweet.text,
                #str(tweet.location),
                tweet.in_reply_to_screen_name,
                tweet.in_reply_to_user_id,
                tweet.in_reply_to_status_id,
                boolToInt(tweet.truncated),
                tweet.source,
                str(tweet.entities.urls),
                str(tweet.entities.user_mentions),
                str(tweet.entities.hashtags),
                str(tweet.geo),
                str(tweet.place),
                str(tweet.coordinates),
                str(tweet.contributors),
                boolToInt(tweet.retweeted),
                #str(tweet.retweeted_status),
                tweet.retweet_count,
                tweet.user.id,
                tweet.user.name,
                tweet.user.screen_name,
                tweet.user.location,
                tweet.user.description,
                tweet.user.profile_image_url,
                boolToInt(tweet.user.profile_background_tile),
                tweet.user.profile_background_image_url,
                tweet.user.profile_sidebar_fill_color,
                tweet.user.profile_background_color,
                tweet.user.profile_link_color,
                tweet.user.profile_text_color,
                boolToInt(tweet.user.protected),
                tweet.user.utc_offset,
                tweet.user.time_zone,
                tweet.user.followers_count,
                tweet.user.friends_count,
                tweet.user.statuses_count,
                tweet.user.favourites_count,
                tweet.user.url,
                #tweet.user.status,
                boolToInt(tweet.user.geo_enabled),
                boolToInt(tweet.user.verified),
                tweet.user.lang,
                boolToInt(tweet.user.notifications),
                boolToInt(tweet.user.contributors_enabled),
                tweet.user.created_at,
                tweet.user.listed_count,))
            print ("Expanded tweet with id:", tweet.id)
        else:
            print ("Tweet with id:", tweet.id, " is already expanded")
        #Add entry in nav tweets-searches
        self.insert_tweets_search(tweet.id, search.id)

    ## Insert search in the db
    def insert_search(self, search):
        if not self.exist_search(search.id):
            self.cur.execute('INSERT INTO '+ self.db_searches_table +' VALUES('
                '?, ?, ?) ',
                (search.id,
                search.created_at,
                search.query,))
            print ("Initialised search")
        else:
            print ("Using previously initalised search")

    ## Insert tweets_serches entry in the db
    def insert_tweets_search(self, tweet_id, search_id):
        if not self.exist_tweets_searches(tweet_id, search_id):
            nav_name = self.db_tweets_table + "_" + self.db_searches_table
            self.cur.execute('INSERT INTO '+ nav_name +' VALUES('
                'NULL, ?, ?) ',
                (tweet_id,
                search_id,))
            print ("Associated tweet with current search")
        else:
            print ("Tweet with id:", tweet_id, " is already associated with the current search")

    ## Determine if a tweet exists in the db. 
    ## Returns
    ##   False if tweet does not exist otherwise True.
    def exist_tweet(self, tweet_id):
        rows = self.cur.execute("SELECT id FROM " + self.db_tweets_table 
                        + " WHERE id = ?", (tweet_id, )).fetchall()
        return (len(rows) > 0)

    ## Determine if a sarch entry is already in the db
    ## Returns
    ##   False if search does not exist otherwise True.
    def exist_search(self, search_id):
        rows = self.cur.execute("SELECT id FROM " + self.db_searches_table
                        + " WHERE id = ?", 
               (search_id, )).fetchall()
        return (len(rows) > 0)

    ## Determine if a tweets exists in the current search
    ## Returns
    ##   False if tweet_search does not exist otherwise True.
    def exist_tweets_searches(self, tweet_id, search_id):
        nav_name = self.db_tweets_table + "_" + self.db_searches_table
        rows = self.cur.execute("SELECT id FROM " + nav_name
                        + " WHERE tweet_id = ? AND search_id = ?", 
               (tweet_id, search_id, )).fetchall()
        return (len(rows) > 0)

class mongo_db(object):
    #TODO: Allow multiple searches!!
    def __init__(self, address, port, collection):
        self.address = address
        self.port = port
        self.collection = collection

    def connect(self):
        self.client = MongoClient(self.address, self.port)
        self.db = self.client[self.collection]

    def close(self):
        self.client = None
        self.db = None

    def insert_tweet(self, tweet, eventindex = 0):
        tweet_json = tweet.__dict__
        tweet_json2 = json.dumps(tweet, default=lambda x:x.__dict__)
        value = json.loads(tweet_json2)
        self.db.posts.insert(value)

    def exist_tweet(self, tweet_id):
        tweet = self.db.posts.find_one( { "_id" : tweet_id} )
        if tweet == None:
            return False
        return True

