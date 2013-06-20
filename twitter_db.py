from search import Search
import requests
import json

class Webservice(object):
    #TODO: clean and order
    def __init__(self, urlbase):
        self.urlbase = urlbase
        self.urlInsertTweet = urlbase + "/tweets.json"
        self.urlInsertTweetSearch = urlbase + "/tweetsearches.json"

    def postTweet(self, tweet, search_id):
        data = tweet.toHash()
        r = requests.post(self.urlInsertTweet, data)
        if r.status_code != 201:
            raise Exception("Post tweet failed, status code = " +
                                    str(r.status_code) + ", message = " + r.text)
    
    def postTweetSearch(self, tweet, search_id):
        data = { "tweet_id":  tweet.id , "search_id": search_id }
        r = requests.post(self.urlInsertTweetSearch, data)
        if r.status_code != 201:
            raise Exception("Post tweet_search failed, " + "status code = " + str(r.status_code) + ", message = " + r.text)

    def headTweet(self, tweet):
        url = self.urlbase + "/tweets/" + str(tweet.id) + ".json"
        r = requests.head(url)
        if r.status_code == 200:
            return True
        elif r.status_code == 404:
            return False
        else:
            raise Exception("Head tweet failed, " + "status code = " + str(r.status_code))

    def getTweetSearchParams(self, tweet, search_id):
        url = self.urlbase + "/tweetsearches.json?search_id=" + str(search_id) + "&tweet_id=" + str(tweet.id)
        r = requests.get(url)
        if r.status_code == 200:
            if not r.text == "[]":
                return True
        return False

class WebserviceDb(object):
    def __init__(self, urlbase):
        self.service = Webservice(urlbase)

    def insertTweet(self, tweet, search_id):
        if self.service.getTweetSearchParams(tweet, search_id):
            print ("Tweet with id:", tweet.id, 
                    " is already associated with the current search")
        elif self.service.headTweet(tweet):
            print ("Tweet with id:", tweet.id, " is already expanded")
            try:
                self.service.postTweetSearch(tweet, search_id)
                print ("Tweet with id:", tweet.id, " associated with ", search_id)
            except Exception as e:
                print ("Insertion tweet_search failed")
                print (e)
        else:
            try:
                self.service.postTweet(tweet, search_id)
                self.service.postTweetSearch(tweet, search_id)
                print ("Tweet with id:", tweet.id, " expanded")
            except Exception as e:
                print ("Insertion tweet failed")
                print (e)

