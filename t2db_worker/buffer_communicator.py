import time

from threading import Lock
from threading import Thread
from threading import Timer

from t2db_objects import objects
from t2db_objects import psocket

from t2db_worker import parser 

from t2db_objects.objects import encodeObject

# Send data to buffer
class BufferCommunicator(object):
    def __init__(self, host, port, test = False):
        self.host = host
        self.port = port
        self.test = test # Used only to avoid communication in test env.

    def sendData(self, tweetList, userList, tweetStreamingList, tweetSearchList):
        # If test is not set, then send the lists to global buffer
        if not self.test:
            try:
                sock = psocket.SocketClient(self.host
                        , self.port).getSocketControl()
                try:
                    sock.sendObject(tweetList)
                    sock.sendObject(userList)
                    sock.sendObject(tweetStreamingList)
                    sock.sendObject(tweetSearchList)
                except Exception as e:
                    print("Error: " + str(e))
                finally:
                    sock.close()
            except Exception as e:
                print("Error: " + str(e))

# Store data produced by worker in local memory.
class LocalBuffer(object):
    def __init__(self):
        self.tweetList = objects.ObjectList()
        self.userList = objects.ObjectList()
        self.tweetStreamingList = objects.ObjectList()
        self.tweetSearchList = objects.ObjectList()

    # Check if the tweet exist in the list and add it
    def addTweet(self, tweet):
        #TODO: determine by ID only
        basicList = self.tweetList.getList()
        for oldTweet in basicList:
            if tweet.id == oldTweet.id :
                raise Exception("The tweet already exists in the list")
        self.tweetList.append(tweet)

    # Check if the user exist in the list and add it
    def addUser(self, user):
        basicList = self.userList.getList()
        for oldUser in basicList:
            if user.id == oldUser.id :
                raise Exception("The user already exists in the list")
        self.userList.append(user)

    # Check if the tweetStream exist in the list and add it
    def addTweetStreaming(self, tweetStreaming):
        basicList = self.tweetStreamingList.getList()
        for oldTweetStreaming in basicList:
            if (tweetStreaming.tweet == oldTweetStreaming.tweet and
                tweetStreaming.streaming == oldTweetStreaming.streaming):
                raise Exception("The tweetStreaming already exists in the list")
        self.tweetStreamingList.append(tweetStreaming)

    # Check if the tweetSearch exist in the list and add it
    def addTweetSearch(self, tweetSearch):
        basicList = self.tweetSearchList.getList()
        for oldTweetSearch in basicList:
            if (tweetSearch.tweet == oldTweetSearch.tweet and
                tweetSearch.search == oldTweetSearch.search):
                raise Exception("The tweetSearch already exists in the list")
        self.tweetSearchList.append(tweetSearch)

# Send data to global buffer if tweet list has reached maxsize
def counter(maxsize, myBuffer, bufferCommunicator):
    if len(myBuffer.localBuffer.tweetList.list) > maxsize :
        # Get data and clean. Thread-safe operation
        bufferCopy = myBuffer.getDataAndClean()
        # Send data
        if len(bufferCopy.tweetList.list) > maxsize:
            bufferCommunicator.sendData(bufferCopy.tweetList
                , bufferCopy.userList, bufferCopy.tweetStreamingList
                , bufferCopy.tweetSearchList)

# Send data to the global buffer in fixed time periods.
def timer(myBuffer, bufferCommunicator):
    # Get data and clean. Thread-safe operation
    bufferCopy = myBuffer.getDataAndClean()
    # Send data
    if len(bufferCopy.tweetList.list) > 0 :
        bufferCommunicator.sendData(bufferCopy.tweetList
            , bufferCopy.userList, bufferCopy.tweetStreamingList
            , bufferCopy.tweetSearchList)

# Tansform from rawTweet to tweet data structure
def status2Tweet(status):
    ps = parser.ParserStatus(status)
    rawTweet = ps.getTweet()
    if type(rawTweet) is not dict:
        raise Exception("Raw tweet is not hash")
    tweet = objects.Tweet(rawTweet)
    return tweet

# Transform from rawTweet to user data structure
def status2User(status):
    ps = parser.ParserStatus(status)
    rawUser = ps.getUser()
    if type(rawUser) is not dict:
        raise Exception("Raw user is not hash")
    user = objects.User(rawUser)
    return user

class Buffer(object):
    # test var only for testing. Avoid communication
    def __init__(self, seconds, maxsize, host, port, test = False):
        self.seconds = seconds
        self.maxsize = maxsize
        self.localBuffer = LocalBuffer()
        self.bufferLock = Lock()
        self.bufferCommunicator = BufferCommunicator(host, port, test)
        self.timerThread = Timer(self.seconds, timer, 
                                    [self, self.bufferCommunicator])
    
    def startTimer(self):
        self.timerThread.start()

    def stopTimer(self):
        self.timerThread.cancel()

    # Get data and clean buffer. Thread-safe
    def getDataAndClean(self):
        self.bufferLock.acquire()
        localBufferCopy = self.localBuffer
        self.localBuffer = LocalBuffer()
        self.bufferLock.release()
        return localBufferCopy

    # Save data. Thread-safe
    def saveData(self, tweet, user, streamingId = None , searchId = None):
        self.bufferLock.acquire()
        try:
            self.localBuffer.addTweet(tweet)
            self.localBuffer.addUser(user)
            if streamingId is not None:
                rawTweetStreaming = {}
                rawTweetStreaming["tweet"] = tweet.id
                rawTweetStreaming["streaming"] = streamingId
                tweetStreaming = objects.TweetStreaming(rawTweetStreaming)
                self.localBuffer.addTweetStreaming(tweetStreaming)
            if searchId is not None:
                rawTweetSearch = {}
                rawTweetSearch["tweet"] = tweet.id
                rawTweetSearch["search"] = searchId
                tweetSearch = objects.TweetSearch(rawTweetSearch)
                self.localBuffer.addTweetSearch(tweetSearch)
        except Exception as e:
            print("Warning: " + str(e))
        self.bufferLock.release()
        # Verify maxsize condition for buffer
        counter(self.maxsize, self, self.bufferCommunicator)

    # Parse the rawTweet and insert each section in the specific list
    def insertTweetStreaming(self, rawTweet, streamingId):
        # Parse
        tweet = status2Tweet(rawTweet)
        user = status2User(rawTweet)
        # Add to buffer
        self.saveData(tweet, user, streamingId, None)        

    # Parse the rawTweet and insert each section in the specific list
    def insertTweetSearch(self, status, searchId):
        # Parse
        tweet = status2Tweet(status)
        user = status2User(status)
        # Add to buffer
        self.saveData(tweet, user, None, searchId)

