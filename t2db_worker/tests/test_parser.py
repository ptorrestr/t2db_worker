#!/usr/bin/env python3
# api.py test

import unittest

from t2db_worker.parser import statusTweet
from t2db_worker.parser import statusUser
from t2db_worker.parser import getElement
from t2db_worker.parser import getRaw
from t2db_worker.parser import ParserStatus

from t2db_worker.tests.test_api import createApiStreaming

def getOneStatus():
    api = createApiStreaming()
    tweetIterator = api.getStream("obama")
    for tweet in tweetIterator:
        status = tweet
        break
    return status

status = None

class TestParserStatus(unittest.TestCase):
    def setUp(self):
        global status
        if status is None:
            status = getOneStatus()

    def test_getTweet(self):
        ps = ParserStatus(status)
        rawTweet = ps.getTweet()
        self.assertIsNotNone(rawTweet)

    def test_getTweetFail(self):
        badStatus = {}
        ps = ParserStatus(badStatus)
        self.assertRaises(Exception, ps.getTweet)

    def test_getUser(self):
        ps = ParserStatus(status)
        rawUser = ps.getUser()
        self.assertIsNotNone(rawUser)

    def test_getUserFail(self):
        badStatus = {}
        ps = ParserStatus(badStatus)
        self.assertRaises(Exception, ps.getUser)

