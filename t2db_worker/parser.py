import json

statusTweet = [
    {"path":"/id","name":"id", "plain":True},
    {"path":"/retweet_count","name":"retweet_count", "plain":True},
    {"path":"/created_at","name":"created_at", "plain":True},
    {"path":"/text", "name":"text", "plain":True},
    {"path":"/in_reply_to_screen_name","name":"in_reply_to_screen_name", "plain":True},
    {"path":"/in_reply_to_user_id", "name":"in_reply_to_user_id", "plain":True},
    {"path":"/in_reply_to_status_id", "name":"in_reply_to_status_id", "plain":True},
    {"path":"/source", "name":"source", "plain":True},
    {"path":"/entities/urls", "name":"urls", "plain":False},
    {"path":"/entities/user_mentions", "name":"user_mentions", "plain":False}, 
    {"path":"/entities/hashtags", "name":"hashtags", "plain":False},
    {"path":"/coordinates", "name":"coordinates", "plain":False},
    {"path":"/contributors", "name":"contributors", "plain":False},
    {"path":"/favorited", "name":"favorited", "plain":True},
    {"path":"/truncated", "name":"truncated", "plain":True},
    {"path":"/retweeted", "name":"retweeted", "plain":True},
    {"path":"/user/id", "name":"user", "plain":True},
    ]

statusUser = [
    {"path":"/user/id", "name":"id", "plain":True},
    {"path":"/user/utc_offset", "name":"utc_offset", "plain":True},
    {"path":"/user/followers_count", "name":"followers_count", "plain":True},
    {"path":"/user/friends_count", "name":"friends_count", "plain":True}, 
    {"path":"/user/statuses_count", "name":"statuses_count", "plain":True},
    {"path":"/user/favourites_count", "name":"favourites_count", "plain":True},
    {"path":"/user/listed_count", "name":"listed_count", "plain":True},
    {"path":"/user/created_at", "name":"created_at", "plain":True},
    {"path":"/user/name", "name":"name", "plain":True},
    {"path":"/user/screen_name", "name":"screen_name", "plain":True}, 
    {"path":"/user/location", "name":"location", "plain":True}, 
    #{"path":"/user/description", "name":"description", "plain":True},
    {"path":"/user/profile_image_url", "name":"profile_image_url", "plain":True},
    {"path":"/user/profile_image_url_https", "name":"profile_image_url_https", "plain":True},
    {"path":"/user/profile_background_image_url", "name":"profile_background_image_url", "plain":True},
    {"path":"/user/profile_background_color", "name":"profile_background_color", "plain":True},
    {"path":"/user/profile_sidebar_fill_color", "name":"profile_sidebar_fill_color", "plain":True},
    {"path":"/user/profile_sidebar_border_color", "name":"profile_sidebar_border_color", "plain":True},
    {"path":"/user/profile_link_color", "name":"profile_link_color", "plain":True},
    {"path":"/user/profile_text_color", "name":"profile_text_color", "plain":True},
    {"path":"/user/time_zone", "name":"time_zone", "plain":True},
    {"path":"/user/url", "name":"url", "plain":True},
    {"path":"/user/lang", "name":"lang", "plain":True},
    {"path":"/user/profile_background_tile", "name":"profile_background_tile", "plain":True},
    {"path":"/user/protected", "name":"protected", "plain":True},
    {"path":"/user/geo_enabled", "name":"geo_enabled", "plain":True},
    {"path":"/user/verified", "name":"verified", "plain":True},
    {"path":"/user/notifications", "name":"notifications", "plain":True},
    {"path":"/user/contributors_enabled", "name":"contributors_enabled", "plain":True},
    ]

# Get elements from the dict returned by Twitter API. Return the raw structure
def getElement(path, status):
    #TODO: add support for list control (*)
    pathList = path.strip().split("/")
    statusLevel = status
    currentPath = "/"
    for subPath in pathList:
        if subPath == "": # don't consider first subPath "".
            continue
        if statusLevel == None:
            raise Exception("Status level is None in status at " + currentPath)
        if not subPath in statusLevel:
            raise Exception("subPath= " + subPath + ", does not exist in " + 
                "status at " + currentPath)
        statusLevel = statusLevel[subPath]
        currentPath += subPath
    return statusLevel

def getRaw(statusObject, status):
    rawTweet = {}
    for element in statusObject:
        statusLevel = getElement(element["path"], status)
        if  element["plain"] == False:
            #The element is not POD, make string with json.
            statusLevel = json.dumps(statusLevel)
        rawTweet[element["name"]] = statusLevel
    return rawTweet    

class ParserStatus(object):
    def __init__(self, status):
        self.status = status

    def getTweet(self):
        rawTweet = getRaw(statusTweet, self.status)
        return rawTweet

    def getUser(self):
        rawUser = getRaw(statusUser, self.status)
        return rawUser
        
