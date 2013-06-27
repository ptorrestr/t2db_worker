from search import Search
import requests
import json

class Webservice(object):
    #TODO: clean and order
    def __init__(self, urlbase):
        self.urlbase = urlbase
        self.urlInsertTweet = urlbase + "/tweets.json"
        self.urlInsertTweetSearch = urlbase + "/tweetsearches.json"
        self.urlInsertUser = urlbase + "/users.json"
        self.auth = ('quiltro', 'perroCallejero')

    def postUser(self, user):
        data = user.toHash()
        r = requests.post(self.urlInsertUser, data, auth = self.auth)
        if r.status_code != 201:
            raise Exception("Post user failed, status code = " +
                                str(r.status_code) + ", message = " + r.text)

    def postTweet(self, tweet):
        data = tweet.toHash()
        r = requests.post(self.urlInsertTweet, data, auth = self.auth)
        if r.status_code != 201:
            raise Exception("Post tweet failed, status code = " +
                                str(r.status_code) + ", message = " + r.text)
    
    def postTweetSearch(self, tweet, search_id):
        data = { "tweet_id":  tweet.id , "search_id": search_id }
        r = requests.post(self.urlInsertTweetSearch, data, auth = self.auth)
        if r.status_code != 201:
            raise Exception("Post tweet_search failed, " + "status code = " + str(r.status_code) + ", message = " + r.text)

    def headUser(self, user):
        url = self.urlbase + "/users/" + str(user.id) + ".json"
        r = requests.head(url, auth = self.auth)
        if r.status_code == 200:
            return True
        elif r.status_code == 404:
            return False
        else:
            raise Exception("Head user failed, status code = " + 
                                str(r.status_code))

    def headTweet(self, tweet):
        url = self.urlbase + "/tweets/" + str(tweet.id) + ".json"
        r = requests.head(url, auth = self.auth)
        if r.status_code == 200:
            return True
        elif r.status_code == 404:
            return False
        else:
            raise Exception("Head tweet failed, status code = " +
                                str(r.status_code))

    def getTweetSearchParams(self, tweet, search_id):
        url = self.urlbase + "/tweetsearches.json?search_id=" + str(search_id) + "&tweet_id=" + str(tweet.id)
        r = requests.get(url, auth = self.auth)
        if r.status_code == 200:
            if not r.text == "[]":
                return True
        return False

class WebserviceDb(object):
    def __init__(self, urlbase):
        self.service = Webservice(urlbase)

    def insertTweet(self, tweet, search_id):
        try:
            if not self.service.headUser(tweet.user):
                print("Adding new user id '", tweet.user.id, "'")
                self.service.postUser(tweet.user)

            if not self.service.headTweet(tweet):
                print("Expanding new tweet id '", tweet.id, "'")
                self.service.postTweet(tweet)

            if not self.service.getTweetSearchParams(tweet, search_id):
                print("Linking tweet id'", tweet.id, "' with current search")
                self.service.postTweetSearch(tweet, search_id)
        except Exception as e:
            print("Tweet insertion failed")
            print(e)
