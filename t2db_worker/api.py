#Wrapper for twitter api. Install with: easy_install3 twitter
from twitter import *
from t2db_objects import objects
import time

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
            print("resettime = ", resettime, " time.time() = ", time.time(),
                " hit = ", hitsremaining)
            diff = int(resettime) - time.time()
            if hitsremaining > 0 and diff > 0:
                break
            #Sleep to give time for twitter API to ask for limits again
            time.sleep(4) 
        sleeptime = int((int(resettime) - time.time()) / int(hitsremaining))
        print("Sleeping", sleeptime, "seconds between API hits until",resettime)
        return sleeptime, resettime

statusTweet = [
    {"path":"/id","name":"id", "kind":"mandatory", "type":int},
    {"path":"/retweet_count","name":"retweet_count", "kind":"non-mandatory", "type":int},
    {"path":"/created_at","name":"created_at", "kind":"mandatory", "type":str},
    {"path":"/kind", "name":"text", "kind":"non-mandatory", "type":str},
    {"path":"/in_reply_to_screen_name","name":"in_reply_to_screen_name", "kind":"non-mandatory", "type":str},
    {"path":"/in_reply_to_user_id", "name":"in_reply_to_user_id", "kind":"non-mandatory", "type":str},
    {"path":"/in_reply_to_status_id", "name":"in_reply_to_status_id", "kind":"non-mandatory", "type":str},
    {"path":"/source", "name":"source", "kind":"non-mandatory", "type":str},
    {"path":"/urls", "name":"urls", "kind":"non-mandatory", "type":str},
    {"name":"user_mentions", "kind":"non-mandatory", "type":str}, 
    {"name":"hashtags", "kind":"non-mandatory", "type":str},
    {"name":"geo", "kind":"non-mandatory", "type":str},
    {"name":"place", "kind":"non-mandatory", "type":str},
    {"name":"coordinates", "kind":"non-mandatory", "type":str},
    {"name":"contributors", "kind":"non-mandatory", "type":str},
    {"name":"favorited", "kind":"non-mandatory", "type":bool},
    {"name":"truncated", "kind":"non-mandatory", "type":bool},
    {"name":"retweeted", "kind":"non-mandatory", "type":bool},
    {"name":"user", "kind":"mandatory", "type":int},
    ]

rawTweetFields = [
    {"name":"annotations", "kind":"non-mandatory", "type":dict},#unused
    {"name":"contributors", "kind":"mandatory", "type":list},#null,Contributors object
    {"name":"coordinates", "kind":"mandatory", "type":list},#null,Coordinates object
    {"name":"created_at", "kind":"mandatory", "type":str},#time
    {"name":"current_user_retweet", "kind":"non-mandatory", "type":dict},#persp, Retweet, not too clear
    {"name":"entities", "kind":"mandatory", "type":dict},#Entitie object
    {"name":"favorite_count", "kind":"mandatory", "type":int},#null
    {"name":"favorited", "kind":"mandatory", "type":bool},#null
    {"name":"filter_level", "kind":"mandatory", "type":str},
    {"name":"id", "kind":"mandatory", "type":int},
    {"name":"id_str", "kind":"mandatory", "type":str},
    {"name":"in_reply_to_screen_name", "kind":"mandatory", "type":str},#null
    {"name":"in_reply_to_status_id", "kind":"mandatory", "type":int},#null
    {"name":"in_reply_to_status_id_str", "kind":"mandatory", "type":str},#null
    {"name":"in_reply_to_user_id", "kind":"mandatory", "type":int},#null
    {"name":"in_reply_to_user_id_str", "kind":"mandatory", "type":str},#null
    {"name":"lang", "kind":"mandatory", "type":str},#null,Not present in Streaming
    {"name":"place", "kind":"mandatory", "type":dict}, #null, Place object
    {"name":"possibly_sensitive", "kind":"non-mandatory", "type":bool},#null
    {"name":"scopes", "kind":"non-mandatory", "type":dict}, #scope, not to clear
    {"name":"retweet_count", "kind":"mandatory", "type":int},
    {"name":"retweeted", "kind":"mandatory", "type":bool},#persp
    {"name":"retweeted_status", "kind":"non-mandatory", "type":dict},#Tweet object
    {"name":"source", "kind":"mandatory", "type":str},
    {"name":"text", "kind":"mandatory", "type":str},
    {"name":"truncated", "kind":"mandatory", "type":bool},
    {"name":"user", "kind":"mandatory", "type":dict},#User object
    {"name":"withheld_copyright", "kind":"non-mandatory", "type":bool},
    {"name":"withheld_in_countries", "kind":"non-mandatory", "type":list},#list of str
    {"name":"withheld_scope", "kind":"non-mandatory", "type":str},

    #{"name":"geo", "kind":"mandatory", "type":str}, #deprecated
    ]

rawContributorsFields = [
    {"name":"id", "kind":"mandatory", "type":int},
    {"name":"id_str", "kind":"mandatory", "type":str},
    {"name":"screen_name", "kind":"mandatory", "type":str},
    ]

rawCoordinatesFields = [
    {"name":"coordinates", "kind":"mandatory", "type":list},
    {"name":"type", "kind":"mandatory", "type":str},
    ]

rawEntitiesFields = [
    {"name":"hashtags", "kind":"mandatory", "type":list},#list of hashtags
    {"name":"media", "kind":"non-mandatory", "type":list},#list of media
    {"name":"urls", "kind":"mandatory", "type":list},#list of urls
    {"name":"user_mentions", "kind":"mandatory", "type":list},#list of userMentions
    ]

rawHashtagsFields = [
    {"name":"indices", "kind":"mandatory", "type":list},#lisf of int
    {"name":"text", "kind":"mandatory", "type":str},
    ]

rawMediaFields = [
    {"name":"display_url", "kind":"mandatory", "type":str},
    {"name":"expanded_url", "kind":"mandatory", "type":str},
    {"name":"id", "kind":"mandatory", "type":int},
    {"name":"id_str", "kind":"mandatory", "type":str},
    {"name":"indices", "kind":"mandatory", "type":list},#list of int
    {"name":"media_url", "kind":"mandatory", "type":str},
    {"name":"media_url_https", "kind":"mandatory", "type":str},
    {"name":"sizes", "kind":"mandatory", "type":dict}, #object sizes
    {"name":"source_status_id", "kind":"mandatory", "type":int},
    {"name":"source_status_id_str", "kind":"mandatory", "type":int},
    {"name":"type", "kind":"mandatory", "type":str},
    {"name":"url", "kind":"mandatory", "type":str},
    ]

rawSizesFields = [
    {"name":"thumb", "kind":"mandatory", "type":dict}, #object size
    {"name":"large", "kind":"mandatory", "type":dict}, #object size
    {"name":"medium", "kind":"mandatory", "type":dict}, #object size
    {"name":"small", "kind":"mandatory", "type":dict}, #object size
    ]

rawSizeFields = [
    {"name":"h", "kind":"mandatory", "type":int},
    {"name":"resize", "kind":"mandatory", "type":str},
    {"name":"w", "kind":"mandatory", "type":str},
    ]

rawUrlFields = [
    {"name":"display_url", "kind":"mandatory", "type":int},
    {"name":"expanded_url", "kind":"mandatory", "type":str},
    {"name":"indices", "kind":"mandatory", "type":list}, #list of int
    {"name":"url", "kind":"mandatory", "type":str},
    ]

rawUserMentionFields = [
    {"name":"id", "kind":"mandatory", "type":int},
    {"name":"id_str", "kind":"mandatory", "type":str},
    {"name":"inidices", "kind":"non-mandatory", "type":list}, #list of int
    {"name":"name", "kind":"mandatory", "type":str},
    {"name":"screen_name", "kind":"mandatory", "type":str},
    ]


rawPlacesFields = [
    {"name":"attributes", "kind":"mandatory", "type":dict}, #geo attributes https://dev.twitter.com/docs/about-geo-place-attributes
    {"name":"bounding_box", "kind":"mandatory", "type":dict}, #bounding box
    {"name":"country", "kind":"mandatory", "type":str},
    {"name":"countr_code", "kind":"mandatory", "type":str},
    {"name":"full_name", "kind":"mandatory", "type":str},
    {"name":"id", "kind":"mandatory", "type":str},
    {"name":"name", "kind":"mandatory", "type":str},
    {"name":"place_type", "kind":"mandatory", "type":str},
    {"name":"url", "kind":"mandatory", "type":str},
    ]

rawBoundingBoxFields = [
    {"name":"coordinates", "kind":"mandatory", "type":list}, # list of list of list of float
    {"name":"type", "kind":"mandatory", "type":str},
]

rawUserFields = [
    {"name":"contributors_enabled", "kind":"mandatory", "type":bool},
    {"name":"created_at", "kind":"mandatory", "type":str}, #date
    {"name":"default_profile", "kind":"mandatory", "type":bool},
    {"name":"default_profile_image", "kind":"mandatory", "type":bool},
    {"name":"description", "kind":"mandatory", "type":str},#null
    {"name":"entities", "kind":"non-mandatory", "type":dict},#user entitie object
    {"name":"favourites_count", "kind":"mandatory", "type":int},
    {"name":"follow_request_sent", "kind":"mandatory", "type":bool}, #null, persp
    {"name":"following", "kind":"mandatory", "type":bool}, #null, persp, depre
    {"name":"followers_count", "kind":"mandatory", "type":int},
    {"name":"friends_count", "kind":"mandatory", "type":int},
    {"name":"geo_enabled", "kind":"mandatory", "type":str},
    {"name":"id", "kind":"mandatory", "type":int},
    {"name":"id_str", "kind":"mandatory", "type":str},
    {"name":"is_translator", "kind":"mandatory", "type":bool},
    {"name":"lang", "kind":"mandatory", "type":str},
    {"name":"listed_count", "kind":"mandatory", "type":int},
    {"name":"location", "kind":"mandatory", "type":str}, #null
    {"name":"name", "kind":"mandatory", "type":str},
    {"name":"notifications", "kind":"mandatory", "type":bool}, #null, depre
    {"name":"profile_background_color", "kind":"mandatory", "type":str},
    {"name":"profile_background_image_url", "kind":"mandatory", "type":str},
    {"name":"profile_background_image_url_https", "kind":"mandatory", "type":str},
    {"name":"profile_background_tile", "kind":"mandatory", "type":bool},
    {"name":"profile_banner_url", "kind":"non-mandatory", "type":str},
    {"name":"profile_image_url", "kind":"mandatory", "type":str},
    {"name":"profile_image_url_https", "kind":"mandatory", "type":str},
    {"name":"profile_link_color", "kind":"mandatory", "type":str},
    {"name":"profile_sidebar_border_color", "kind":"mandatory", "type":str},
    {"name":"profile_sidebar_fill_color", "kind":"mandatory", "type":str},
    {"name":"profile_text_color", "kind":"mandatory", "type":str},
    {"name":"profile_use_background_image", "kind":"mandatory", "type":bool},
    {"name":"protected", "kind":"mandatory", "type":bool},
    {"name":"screen_name", "kind":"mandatory", "type":str},
    {"name":"show_all_inline_media", "kind":"non-mandatory", "type":bool},
    {"name":"status", "kind":"non-mandatory", "type":dict}, #Object tweet. null, miss    
    {"name":"statuses_count", "kind":"mandatory", "type":int},
    {"name":"time_zone", "kind":"mandatory", "type":str}, #null
    {"name":"url", "kind":"mandatory", "type":str},# null
    {"name":"utc_offset", "kind":"mandatory", "type":int}, #null
    {"name":"verified", "kind":"mandatory", "type":bool},
    {"name":"withheld_in_countries", "kind":"non-mandatory", "type":str},
    {"name":"withheld_scope", "kind":"non-mandatory", "type":str},
    ]

rawUserEntitiesFields = [
    {"name":"url", "kind":"mandatory", "type":dict}, # rawUserEntities object
    {"name":"description", "kind":"mandatory", "type":dict},
    ]

rawUserEntitiesUrlFields = [
    {"name":"urls", "kind":"mandatory", "type":list}, #list of rawUrlFields
    ]

#Data structures imported from Twitter API
dataStructuresTwitter = [
    rawTweetFields,
    rawContributorsFields,
    rawCoordinatesFields,
    rawEntitiesFields,
    rawHashtagsFields,
    rawMediaFields,
    rawSizesFields,
    rawSizeFields,
    rawUrlFields,
    rawUserMentionFields,
    rawPlacesFields,
    rawBoundingBoxFields,
    rawUserFields,
    rawUserEntitiesFields,
    rawUserEntitiesUrlFields,
    ]

# Tweets directly recieved by API are stored with this format. 
class RawTweet(objects.Object):
    def __init__(self, rawTweet):
        try:
            super(RawTweet, self).__init__(rawTweetFields, rawTweet)
            self.entities = RawEntities(self.entities)
            self.metadata = RawMetadata(self.metadata)
            self.user = RawUser(self.user)
        except Exception as e:
            raise Exception("RawTweet creation failed: " + str(e))

    def equal(self, rObject):
        return super(RawTweet, self).equal(rObject)

    def equalHash(self, rHash):
        return super(RawTweet, self).equalHash(rHash)

    #def toHash(self):
    #    args = super(RawTweet, self).toHash()
    #    return args

    def toHash(self):
        args = {}
        args['id'] = self.id 
        args['created_at'] = self.created_at
        args['favorited'] = self.favorited
        args['text'] = self.text
        args['in_reply_to_screen_name'] = self.in_reply_to_screen_name
        args['in_reply_to_user_id'] = self.in_reply_to_user_id
        args['in_reply_to_status_id'] = self.in_reply_to_status_id
        args['truncated'] = self.truncated
        args['source'] = self.source
        args['urls'] = self.entities.urls
        args['user_mentions'] = self.entities.user_mentions 
        args['hashtags'] = self.entities.hashtags
        args['geo'] = self.geo
        args['place'] = self.place
        args['coordinates'] = self.coordinates
        args['contributors'] = self.contributors
        args['retweeted'] = self.retweeted
        args['retweet_count'] = self.retweet_count
        args['user'] = self.user.id
        return args

class RawEntities(objects.Object):
    def __init__(self, rawEntities):
        try:
            super(RawEntities, self).__init__(rawEntitiesFields, rawEntities)
        except Exception as e:
            raise Exception("RawEntities creation failed: " + str(e))

    def equal(self, rObject):
        return super(RawEntities, self).equal(rObject)

    def equalHash(self, rHash):
        return super(RawEntities, self).equalHash(rHash)

    def toHash(self):
        args = super(RawEntities, self).toHash()
        return args

class RawMetadata(objects.Object):
    def __init__(self, rawMetadata):
        try:
            super(RawMetadata, self).__init__(rawMetadataFields, rawMetadata)
        except Exception as e:
            raise Exception("RawMetadata creation failed: " + str(e))

    def equal(self, rObject):
        return super(RawMetadata, self).equal(rObject)

    def equalHash(self, rHash):
        return super(RawMetadata, self).equalHash(rHash)

    def toHash(self):
        args = super(RawMetadata, self).toHash()
        return args

class RawUser(objects.Object):
    def __init__(self, rawUser):
        try:
            super(RawUser, self).__init__(rawUserFields, rawUser)
        except Exception as e:
            raise Exception("Tweet creation failed: " + str(e))

    def equal(self, rObject):
        return super(RawUser, self).equal(rObject)

    def equalHash(self, rHash):
        return super(RawUser, self).equalHash(rHash)

    #def toHash(self):
    #    args = super(RawUser, self).toHash()
    #    return args

    def toHash(self):
        args = {}
        args['id'] = self.id
        args['created_at'] = self.created_at
        args['name'] = self.name
        args['screen_name'] = self.screen_name
        args['location'] = self.location
        args['description'] = self.description
        args['profile_image_url'] = self.profile_image_url
        args['profile_image_url_https'] = self.profile_image_url_https
        args['profile_background_tile'] = boolToInt(self.profile_background_tile)
        args['profile_background_image_url'] = self.profile_background_image_url
        args['profile_background_color'] = self.profile_background_color
        args['profile_sidebar_fill_color'] = self.profile_sidebar_fill_color
        args['profile_sidebar_border_color'] = self.profile_sidebar_border_color
        args['profile_link_color'] = self.profile_link_color
        args['profile_text_color'] = self.profile_text_color
        args['protected'] = boolToInt(self.protected)
        args['utc_offset'] = self.utc_offset
        args['time_zone'] = self.time_zone
        args['followers_count'] = self.followers_count
        args['friends_count'] = self.friends_count
        args['statuses_count'] = self.statuses_count
        args['favourites_count'] = self.favourites_count
        args['url'] = self.url
        args['geo_enabled'] = boolToInt(self.geo_enabled)
        args['verified'] = boolToInt(self.verified)
        args['lang'] = self.lang
        args['notifications'] = boolToInt(self.notifications)
        args['contributors_enabled'] = boolToInt(self.contributors_enabled)
        args['listed_count'] = self.listed_count
        return args
