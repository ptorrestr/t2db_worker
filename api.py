#Wrapper for twitter api
from twitter import *

def toListOfTweets(tweets):
    result = []
    rawTweets = tweets['statuses']
    for rawTweet in rawTweets:
        result.append(Tweet(rawTweet))
    return result

def boolToInt(bool_):
    if (bool_):
        return 1
    else:
        return 0

class ApiStreaming(object):
    def __init__(self,
                    consumer_key,
                    consumer_secret,
                    access_token_key,
                    access_token_secret):
        self.twitter = TwitterStream( auth = OAuth(access_token_key, 
                                             access_token_secret,
                                             consumer_key,
                                             consumer_secret))
    
    def GetStream(self, track = None):
        iterTweets = self.twitter.statuses.filter(track = track)
        return iterTweets

class Api(object):
    
    def __init__(self,
                    consumer_key,
                    consumer_secret,
                    access_token_key,
                    access_token_secret):
        self.twitter = Twitter( auth = OAuth(access_token_key, 
                                             access_token_secret,
                                             consumer_key,
                                             consumer_secret))
        
    def GetSearch(self,
                    q = None,
                    #geocode = None,
                    #lang = None,
                    #locale = None,
                    #result_type = None,
                    count = None,
                    #until = None,
                    since_id = None,
                    #max_id = None,
                    #include_entities = None,
                    #callback = None
                    ):
        tweets = self.twitter.search.tweets(q = q,
                            #geocode = geocode,
                            #lang = lang,
                            #locale = locale,
                            #result_type = result_type,
                            count = count,
                            #until = until,
                            since_id = since_id,
                            #max_id = max_id,
                            #include_entities = include_entities,
                            #callback = callback
                            )

        tweetsList = toListOfTweets(tweets)
        return tweetsList

    def GetSearchRateLimit(self):
        limits = self.twitter.application.rate_limit_status()
        remaining = limits['resources']['search']['/search/tweets']['remaining']
        reset = limits['resources']['search']['/search/tweets']['reset']
        limit = limits['resources']['search']['/search/tweets']['limit']
        return [remaining, reset, limit]

class Tweet(object):
    def __init__(self, rawTweet): 
        self.id = rawTweet['id']
        self.created_at = rawTweet['created_at']
        self.favorited = boolToInt(rawTweet['favorited'])
        self.text = rawTweet['text']
        self.in_reply_to_screen_name = rawTweet['in_reply_to_screen_name']
        self.in_reply_to_user_id = rawTweet['in_reply_to_user_id']
        self.in_reply_to_status_id = rawTweet['in_reply_to_status_id']
        self.truncated = boolToInt(rawTweet['truncated'])
        self.source = rawTweet['source']
        self.urls = rawTweet['entities']['urls']
        self.user_mentions = rawTweet['entities']['user_mentions']
        self.hashtags = rawTweet['entities']['hashtags']
        self.geo = rawTweet['geo']
        self.place = rawTweet['place']
        self.coordinates = rawTweet['coordinates']
        self.contributors = rawTweet['contributors']
        self.retweeted = boolToInt(rawTweet['retweeted'])
        self.retweet_count = rawTweet['retweet_count']
        self.user_id = rawTweet['user']['id']
        self.user_name = rawTweet['user']['name']

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
        args['urls'] = self.urls
        args['user_mentions'] = self.user_mentions 
        args['hashtags'] = self.hashtags
        args['geo'] = self.geo
        args['place'] = self.place
        args['coordinates'] = self.coordinates
        args['contributors'] = self.contributors
        args['retweeted'] = self.retweeted
        args['retweet_count'] = self.retweet_count
        args['user_id'] = self.user_id
        args['user_name'] = self.user_name
        return args

class Entities(object):
    def __init__(self, rawEntities):
        self.urls = rawEntities['urls']
        self.hashtags = rawEntities['hashtags']
        self.user_mentions = rawEntities['user_mentions']

class Metadata(object):
    def __init__(self, rawMetadata):
        self.iso_language_code = rawMetadata['iso_language_code']
        self.result_type = rawMetadata['result_type']

class User(object):
    def __init__(self, rawUser):
        self.profile_sidebar_fill_color = rawUser['profile_sidebar_fill_color']
        self.profile_sidebar_border_color = rawUser['profile_sidebar_fill_color']
        self.profile_background_tile = rawUser['profile_background_tile']
        self.name = rawUser['name']
        self.profile_image_url = rawUser['profile_image_url']
        self.created_at = rawUser['created_at']
        self.location = rawUser['location']
        self.follow_request_sent = rawUser['follow_request_sent']
        self.profile_link_color = rawUser['profile_link_color']
        self.is_translator = rawUser['is_translator']
        self.id_str = rawUser['id_str']
        self.entities = rawUser['entities']
        self.default_profile = rawUser['default_profile']
        self.contributors_enabled = rawUser['contributors_enabled']
        self.favourites_count = rawUser['favourites_count']
        self.url = rawUser['url']
        self.profile_image_url_https = rawUser['profile_image_url_https']
        self.utc_offset = rawUser['utc_offset']
        self.id = rawUser['id']
        self.profile_use_background_image = rawUser['profile_use_background_image']
        self.listed_count = rawUser['listed_count']
        self.profile_text_color = rawUser['profile_text_color']
        self.lang = rawUser['lang']
        self.followers_count = rawUser['followers_count']
        self.protected = rawUser['protected']
        self.notifications = rawUser['notifications']
        self.profile_background_image_url_https = rawUser['profile_background_image_url_https']
        self.profile_background_color = rawUser['profile_background_color']
        self.verified = rawUser['verified']
        self.geo_enabled = rawUser['geo_enabled']
        self.time_zone = rawUser['time_zone']
        self.description = rawUser['description']
        self.default_profile_image = rawUser['default_profile_image']
        self.profile_background_image_url = rawUser['profile_background_image_url']
        self.statuses_count = rawUser['statuses_count']
        self.friends_count = rawUser['friends_count']
        self.following = rawUser['following']
        #self.show_all_inline_media = rawUser['show_all_inline_media']
        self.screen_name = rawUser['screen_name']

