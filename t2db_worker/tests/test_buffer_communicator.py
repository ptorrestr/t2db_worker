import unittest
import time

from t2db_worker.buffer_communicator import BufferCommunicator
from t2db_worker.buffer_communicator import LocalBuffer
from t2db_worker.buffer_communicator import counter
from t2db_worker.buffer_communicator import timer
from t2db_worker.buffer_communicator import status2Tweet
from t2db_worker.buffer_communicator import status2User
from t2db_worker.buffer_communicator import Buffer

from t2db_worker.parser import ParserStatus
from t2db_worker.tests.test_parser import getOneStatus

status = None

class TestStatus2Object(unittest.TestCase):
    def setUp(self):
        global status
        if status is None:
            status = getOneStatus()

    def test_status2Tweet(self):
        global status
        tweet = status2Tweet( status )

    def test_status2User(self):
        global status
        user = status2User( status )

class TestLocalBuffer(unittest.TestCase):
    def setUp(self):
        global status
        if status is None:
            status = getOneStatus()

    def test_addTweet(self):
        global status
        lb = LocalBuffer()
        tweet = status2Tweet( status )
        lb.addTweet(tweet)
        self.assertEqual(len(lb.tweetList.getList()), 1)

    def test_addRepeatedTweet(self):
        global status
        lb = LocalBuffer()
        tweet = status2Tweet( status )
        lb.addTweet(tweet)
        self.assertRaises(Exception, lb.addTweet, tweet)

    def test_addUser(self):
        global status
        lb = LocalBuffer()
        user = status2User( status )
        lb.addUser(user)
        self.assertEqual(len(lb.userList.getList()), 1)

    def test_addRepeatedUser(self):
        global status
        lb = LocalBuffer()
        user = status2User(status)
        lb.addUser(user)
        self.assertRaises(Exception, lb.addUser, user)

    def test_addTweetStreaming(self):
        lb = LocalBuffer()
        lb.addTweetStreaming(1, 1)
        self.assertEqual(len(lb.tweetStreamingList.getList()), 1)

    def test_addRepeatedTweetStreaming(self):
        lb = LocalBuffer()
        lb.addTweetStreaming(1, 1)
        self.assertRaises(Exception, lb.addTweetStreaming, [1, 1])

    def test_addTweetSearch(self):
        lb = LocalBuffer()
        lb.addTweetSearch(1, 1)
        self.assertEqual(len(lb.tweetSearchList.getList()), 1)

    def test_addRepeatedTweetSearch(self):
        lb = LocalBuffer()
        lb.addTweetSearch(1, 1)
        self.assertRaises(Exception, lb.addTweetSearch, [1, 1])
        

class TestBuffer(unittest.TestCase):
    def setUp(self):
        global status
        if status is None:
            status = getOneStatus()

    def test_insertTweetStreaming(self):
        global status
        bf = Buffer(10, 10, "myhost", 100, True)
        for i in range(0, 10):
            status["id"] = i
            status["user"]["id"] = i
            bf.insertTweetStreaming(status, 1)
        self.assertEqual(len(bf.localBuffer.tweetList.getList()), 10)
        self.assertEqual(len(bf.localBuffer.userList.getList()), 10)
        self.assertEqual(len(bf.localBuffer.tweetStreamingList.getList()), 10)

    def test_insertTweetStreamingCounter(self):
        global status
        bf = Buffer(10, 10, "myhost", 100, True)
        for i in range(0, 11):
            status["id"] = i
            status["user"]["id"] = i
            bf.insertTweetStreaming(status, 1)
        self.assertEqual(len(bf.localBuffer.tweetList.getList()), 0)
        self.assertEqual(len(bf.localBuffer.userList.getList()), 0)
        self.assertEqual(len(bf.localBuffer.tweetStreamingList.getList()), 0)

    def test_insertTweetStreamingTimer(self):
        global status
        bf = Buffer(10, 10, "myhost", 100, True)
        start = time.time()
        bf.startTimer()
        for i in range(0, 5):
            status["id"] = i
            status["user"]["id"] = i
            bf.insertTweetStreaming(status, 1)
        end = time.time()
        diff = end - start
        time.sleep(9 - diff)
        self.assertEqual(len(bf.localBuffer.tweetList.getList()), 5)
        bf.stopTimer()

    def test_insertTweetStreamingTimerActive(self):
        global status
        bf = Buffer(10, 10, "myhost", 100, True)
        start = time.time()
        bf.startTimer()
        for i in range(0, 5):
            status["id"] = i
            status["user"]["id"] = i
            bf.insertTweetStreaming(status, 1)
        end = time.time()
        diff = end - start
        time.sleep(10)
        self.assertEqual(len(bf.localBuffer.tweetList.getList()), 0)
        bf.stopTimer()

    def test_insertTweetSearch(self):
        global status
        bf = Buffer(10, 10, "myhost", 100, True)
        for i in range(0, 10):
            status["id"] = i
            status["user"]["id"] = i
            bf.insertTweetSearch(status, 1)
        self.assertEqual(len(bf.localBuffer.tweetList.getList()), 10)
        self.assertEqual(len(bf.localBuffer.userList.getList()), 10)
        self.assertEqual(len(bf.localBuffer.tweetSearchList.getList()), 10)

    def test_insertTweetSearchCounter(self):
        global status
        bf = Buffer(10, 10, "myhost", 100, True)
        for i in range(0, 11):
            status["id"] = i
            status["user"]["id"] = i
            bf.insertTweetSearch(status, 1)
        self.assertEqual(len(bf.localBuffer.tweetList.getList()), 0)
        self.assertEqual(len(bf.localBuffer.userList.getList()), 0)
        self.assertEqual(len(bf.localBuffer.tweetSearchList.getList()), 0)

    def test_insertTweetSearchTimer(self):
        global status
        bf = Buffer(10, 10, "myhost", 100, True)
        start = time.time()
        bf.startTimer()
        for i in range(0, 5):
            status["id"] = i
            status["user"]["id"] = i
            bf.insertTweetSearch(status, 1)
        end = time.time()
        diff = end - start
        time.sleep(9 - diff)
        self.assertEqual(len(bf.localBuffer.tweetList.getList()), 5)
        bf.stopTimer()

    def test_insertTweetSearchTimerActive(self):
        global status
        bf = Buffer(10, 10, "myhost", 100, True)
        start = time.time()
        bf.startTimer()
        for i in range(0, 5):
            status["id"] = i
            status["user"]["id"] = i
            bf.insertTweetSearch(status, 1)
        end = time.time()
        diff = end - start
        time.sleep(10)
        self.assertEqual(len(bf.localBuffer.tweetList.getList()), 0)
        bf.stopTimer()
     
