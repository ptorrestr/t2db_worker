import sys
import argparse 
import signal
import time
import logging

from time import gmtime, strftime
from threading import Event
from threading import Barrier

from t2db_worker.search import Search
from t2db_worker import api
from t2db_worker import buffer_communicator as bc

# create logger
logger = logging.getLogger('Worker')
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

## Use streaming API.
def streaming(search, streaming, bf, stopEvent, barrier):
    # Initialise timer thread in buffer    
    bf.startTimer()
    lastid = 0
    try:
        tweetsIter = streaming.getStream(track = search.query)
        for rawTweet in tweetsIter:
            if stopEvent.isSet():
                logger.info("Stoping streaming")
                break
            bf.insertTweetStreaming(rawTweet, search.id)
            lastid = rawTweet["id"]
    except Exception as err:
        logger.error(str(err))
        logger.debug("Last tweet id read = " + str(lastid))
    # Stop timer thread in buffer
    bf.stopTimer()
    barrier.wait()
    logger.info("Streaming stoped")

## This is the core function of the program. In first step the twitter API
## is initialised. Then, it determines the sleep time for each sucessivily 
## call of twitter search API. Finally, the function performs the recollection
## of tweets, using the ID of the last tweet(the largest ID of the last 
## reccollection) as input for the next recollection. In this manner, the
## function only collects new tweets for the given query.
## Returns:
##   No returns
def searchFuture(search, api, bf, stopEvent, barrier):
    
    ## Main loop. Start collection of tweets
    query_num = 1
    lastid = 0
    while True:
        try:
            if  stopEvent.isSet():
                logger.info("Stoping search")
                break
            ## Get tweets from API
            tweets = api.getSearch(q = search.query, since_id = lastid, 
                count = 100)
            ## For each tweets store in db. Find the largest
            for tweet in tweets:
                ## This method will save tweet in db and associated with 
                ## current search. If the tweets is already stored, it will 
                ## associated with current search. If the tweets is already
                ## associated with this search, it will do nothing
                if not stopEvent.isSet():
                    bf.insertTweetSearch(tweet, search.id)
                    ## find the largest
                    if lastid < tweet["id"]:
                        lastid = tweet["id"]
            logger.debug("Last tweet.id = " + str(lastid) + ", query num = " 
                + str(query_num))
            query_num += 1
        except Exception as err:
            logger.error(str(err))
            logger.debug("Last tweet id read = " + str(lastid))
    # Stop timer thread in buffer
    bf.stopTimer()
    barrier.wait()
    logger.info("Searching stoped")


def searchHistorical(search, api, bf, stopEvent, barrier):
    ## Main loop. Start collection of tweets
    bf.startTimer()
    query_num = 1
    lastid = 0
    first = True
    while True:
        try:
            if  stopEvent.isSet():
                logger.info("StopEvent occurs!")
                break
            ## Get tweets from API
            tweets = api.getSearch(q = search.query, max_id = lastid, 
                count = 100)
            ## For each tweets store in db. Find the least ID
            for tweet in tweets:
                ## This method will save tweet in db and associated with 
                ## current search. If the tweets is already stored, it will 
                ## associated with current search. If the tweets is already
                ## associated with this search, it will do nothing
                if not stopEvent.isSet():
                    bf.insertTweetSearch(tweet, search.id)
                    ## find the least
                    if lastid > tweet["id"]:
                        lastid = tweet["id"]
                    elif first:
                        lastid = tweet["id"]
                        first = False
            logger.debug("Last id = " + str(lastid) + " query = " + str(query_num))
            query_num = query_num + 1
        except Exception as err:
            logger.error(str(err))
            logger.debug("Last tweet id read = " + str(lastid))
    # Stop timer thread in buffer)
    logger.debug("Stoping timer")
    bf.stopTimer()
    logger.debug("Waiting in barrier: " + str(barrier))
    barrier.wait()
    logger.info("Searching stoped")

# Global variables for signal_handler function
gStopEvent = None
gBarrier = None
gFinalise = None

def setGlobalVariable(stopEvent, barrier, finalise):
    global gStopEvent
    global gBarrier
    global gFinalise
    gStopEvent = stopEvent
    gBarrier = barrier
    gFinalise = finalise

## this function controls SIGINT signal (Ctrl+C).
def signal_handler(signal, frame):
    global gStopEvent
    global gBarrier
    global gFinalise
    logger.info ("You pressed Ctrl+C!, stoping")
    gStopEvent.set()
    logger.info ("StopEvent triggered")
    logger.debug("Waiting in barrier: " + str(barrier))
    gBarrier.wait()
    if gFinalise:
        sys.exit(0)

def main():
    ## Start signal detection
    signal.signal(signal.SIGINT, signal_handler)

    ## Parser input arguments
    parser = argparse.ArgumentParser()
    # positionals
    parser.add_argument('--query',
        help='The query used to search tweets',
        type = str,
        required = True)
    parser.add_argument('--api',
        help = 'The twitter api used',
        choices = ['search', 'streaming'],
        required = True)
    # with default
    parser.add_argument('--buffer-server',
        help='The url of the global buffer. e.g. http://localhost',
        default = "http://localhost",
        type = str,
        required = True)
    parser.add_argument('--buffer-server-port',
        help='The port of the global buffer. e.g. 8000',
        default = "8000",
        type = str,
        required = True)
    parser.add_argument('--buffer-maxsize',
        help = 'The maximum size (length) allowed for buffering',
        default = 25,
        type = int,
        required = True)
    parser.add_argument('--buffer-maxtime',
        help = 'The time for each period of buffering',
        default = 25,
        type = int,
        required = True)
    parser.add_argument('--search-id', 
        help = 'The search id for collection mode',
        required = False,
        default = 0,
        type = int)
    parser.add_argument('--con',
        help = 'The consumer credential',
        required = True,
        type = str)
    parser.add_argument('--con_sec',
        help = 'The consumer secret credential',
        required = True,
        type = str)
    parser.add_argument('--acc',
        help = 'The access token credential',
        required = True,
        type = str)
    parser.add_argument('--acc_sec',
        help = 'The access token secret credential',
        required = True,
        type = str)
    args = parser.parse_args()

    ## Create search
    s = Search(args.search_id, args.query)

    ## Initialise twitter API
    if args.api == "search" or args.api == "searchFuture":
        api_ = api.ApiSearch(consumerKey = args.con,
                      consumerSecret = args.con_sec,
                      accTokenKey = args.acc,
                      accTokenSecret = args.acc_sec)
    else:
        api_ = api.ApiStreaming(consumeKey = args.con,
                      consumerSecret = args.con_sec,
                      accTokenKey = args.acc,
                      accTokenSecret = args.acc_sec)
    ## Start buffer
    bf = bc.Buffer(args.buffer_maxtime, args.buffer_maxsize, args.buffer_server
        , args.buffer_server_port)
    # global variables
    stopEvent = Event()
    barrier = Barrier(2)
    finalise = True
    setGlobalVariable(stopEvent, barrier, finalise)
    ## Start program!
    try:
        if args.api == "search":
            searchHistorical(s, api_, bf, stopEvent, barrier)
        elif args.api == "searchFuture":
            searchFuture(s, api_, bf, stopEvent, barrier)
        elif args.api == "streaming":
            streaming(s, api_, bf, stopEvent, barrier)
    except Exception as e:
        logger.error("Program end unexpectely: " + str(e))
        sys.exit(2)
    logger.error("Program ended!")
    ## End program!
    sys.exit(0)
