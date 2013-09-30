#!/usr/bin/env python3
# api.py test

import unittest
import random
import time

from t2db_worker.api import ApiStreaming
from t2db_worker.api import ApiSearch

from t2db_worker.parser import statusTweet
from t2db_worker.parser import statusUser
from t2db_worker.parser import getElement
from t2db_worker.buffer_communicator import status2Tweet 

# TODO: change to variable set of keys
# Use fixed set of keys. 
testConsumerKey = "EfJqVgTtYjBkWYSiTocp4A"
testConsumerSecret = "MF21TGTX6D5vYeLAmkmMR03wqluczehMKWUA2uLMg"
testAccessTokenKey = "279184521-kih1nYimKGo3Md62rMpgUI54pARvGesaJzzm8AuY"
testAccessTokenSecret = "2EFpz65YjkYbONRTyylfM6bIJuUnFAZvhQJjqbifQU"

# List of topics
topicList = ['chile', 'obama', 'merkel', 'syria']

def selectRandomTopic():
    global topicList
    index = random.randint(0, len(topicList)-1)
    return topicList[index]

def createApiStreaming():
    global testConsumerKey
    global testConsumerSecret
    global testAccessTokenKey
    global testAccessTokenSecret
    api = ApiStreaming(testConsumerKey, testConsumerSecret, testAccessTokenKey,
        testAccessTokenSecret)
    return api

def createApiSearch():
    global testConsumerKey
    global testConsumerSecret
    global testAccessTokenKey
    global testAccessTokenSecret
    api = ApiSearch(testConsumerKey, testConsumerSecret, testAccessTokenKey,
        testAccessTokenSecret)
    return api

class TestApiStreaming(unittest.TestCase):
    def setUp(self):
        self.val = 0

#    def test_getData(self):
#        api = createApiStreaming()
#        tweetIterator = api.getStream(selectRandomTopic())
#        for tweet in tweetIterator:
#            self.assertTrue("id" in tweet)
#            break
#
#    def test_validTweetFields(self):
#        api = createApiStreaming()
#        tweetIterator = api.getStream(selectRandomTopic())
#        for tweet in tweetIterator:
#            for element in statusTweet:
#                raw = getElement(element["path"], tweet)
#            break
#
#    def test_validUserFields(self):
#        api = createApiStreaming()
#        tweetIterator = api.getStream(selectRandomTopic())
#        for tweet in tweetIterator:
#            for element in statusUser:
#                raw = getElement(element["path"], tweet)
#            break

    def test_validEncoding(self):
        api = createApiStreaming()
        statusIterator = api.getStream(selectRandomTopic())
        for status in statusIterator:
            print(status["text"])
            tweet = status2Tweet(status)
            print(tweet.text)
            break

    def tearDown(self):
        self.val = 1

#class TestApiSearch(unittest.TestCase):
#    def test_getData(self):
#        api =  createApiSearch()
#        statusList = api.getSearch(q = selectRandomTopic(), count = 10, max_id = 0)
#        for status in statusList:
#            self.assertTrue("id" in status)
#
#    def test_validTweetFields(self):
#        api = createApiSearch()
#        statusList = api.getSearch(q = selectRandomTopic(), count = 10, max_id = 0)
#        for status in statusList:
#            for element in statusTweet:
#                raw = getElement(element["path"], status)
#            break
#
#    def test_validUserFields(self):
#        api = createApiSearch()
#        statusList = api.getSearch(q = selectRandomTopic(), count = 10, max_id = 0)
#        for status in statusList:
#            for element in statusUser:
#                raw = getElement(element["path"], status)
#            break
#
#    def test_sleepTime(self):
#        api = createApiSearch()
#        for i in range(0, 10):
#            start = time.time()
#            statusList = api.getSearch(q = selectRandomTopic(), count = 10, 
#                max_id = 0)
#            end = time.time()
#            self.assertAlmostEqual( (end - start), api.sleepTime, delta = 1)
#

if __name__ == '__main__':
    unittest.main()        
