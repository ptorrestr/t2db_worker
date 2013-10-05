import time
import logging

from twitter import *
from t2db_objects import objects

# create logger
logger = logging.getLogger('Api')
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)

def toListOfTweets(tweets):
    result = []
    rawTweets = tweets['statuses']
    for rawTweet in rawTweets:
        result.append(rawTweet)
    return result

class ApiStreaming(object):
    def __init__(self, consumerKey, consumerSecret, accTokenKey, accTokenSecret):
        oauth = OAuth(accTokenKey, accTokenSecret, consumerKey, consumerSecret)
        self.twitter = TwitterStream( auth = oauth )
    
    def getStream(self, track):
        iterTweets = self.twitter.statuses.filter(track = track)
        return iterTweets

class ApiSearch(object):
    
    def __init__(self, consumerKey, consumerSecret, accTokenKey, accTokenSecret):
        oauth = OAuth(accTokenKey, accTokenSecret, consumerKey, consumerSecret)
        self.twitter = Twitter( auth = oauth )
        [self.sleepTime, self.resetTime] = self.limitRate()

    # More info https://dev.twitter.com/docs/api/1.1/get/search/tweets
    def getSearch(self, q = None, count = None, max_id = None, since_id = None):
        # Is the windows closed?
        if time.time() >= self.resetTime :
            [self.sleepTime, self.resetTime] = limitRate()
        time.sleep(self.sleepTime)
        # Determine if is historical search or future search (similar as stream)
        if since_id is None:
            tweets = self.twitter.search.tweets(q = q, count = count, 
                max_id = max_id)
        else:
            tweets = self.twitter.search.tweets(q = q, count = count, 
                since_id = since_id)
        tweetsList = toListOfTweets(tweets)
        return tweetsList

    def getSearchRateLimit(self):
        limits = self.twitter.application.rate_limit_status()
        remaining = limits['resources']['search']['/search/tweets']['remaining']
        reset = limits['resources']['search']['/search/tweets']['reset']
        limit = limits['resources']['search']['/search/tweets']['limit']
        return [remaining, reset, limit]

    def limitRate(self):
        while True:
            [hitsremaining, resettime, limit] = self.getSearchRateLimit()
            logger.debug("resettime = " + str(resettime) + " time.time() = " + 
                str(time.time()) + " hit = " + str(hitsremaining))
            diff = int(resettime) - time.time()
            if hitsremaining > 0 and diff > 0:
                break
            #Sleep to give time for twitter API to ask for limits again
            time.sleep(4) 
        sleeptime = int((int(resettime) - time.time()) / int(hitsremaining))
        logger.debug("Sleeping " + str(sleeptime) + " seconds between API " +
            "hits until" + str(resettime))
        return sleeptime, resettime
