import unittest
import time
import _thread as thread
from threading import Semaphore
from threading import Event
from threading import Barrier

from t2db_objects.psocket import SocketServer

from t2db_worker import buffer_communicator as bc
from t2db_worker.worker import streaming
from t2db_worker.worker import searchFuture
from t2db_worker.worker import searchHistorical
from t2db_worker.worker import setGlobalVariable
from t2db_worker.worker import signal_handler
from t2db_worker.tests.test_api import selectRandomTopic
from t2db_worker.tests.test_api import createApiStreaming
from t2db_worker.tests.test_api import createApiSearch
from t2db_worker.search import Search

from t2db_objects.objects import parseText

sharedList = []
sharedListLock = Semaphore(0)

barrier = Barrier(3)

hostFakeGlobalBuffer = "galvarino"
portFakeGlobalBuffer = 13001

def fakeGlobalBuffer(port):
    global sharedList
    global sharedListLock
    global barrier
    server = SocketServer(port = port, maxConnection = 5)
    sock = server.accept()
    try:
        tweetList = sock.recvObject()
        userList = sock.recvObject()
        tweetStreamingList = sock.recvObject()
        tweetSearchList = sock.recvObject()
    except Exception as e:
        raise Exception(str(e))
        print("Error fake: " + str(e))
    finally:
        sock.close()
        server.close()
    try:
        sharedList.append(tweetList)
        sharedList.append(userList)
        sharedList.append(tweetStreamingList)
        sharedList.append(tweetSearchList)
    except Exception as e:
        print("Error fake: " + str(e))
    sharedListLock.release()
    barrier.wait()

def fakeSignal(stopEvent, barrier):
    setGlobalVariable(stopEvent, barrier, False)
    signal_handler(None, None)
    
class TestMain(unittest.TestCase):
    def setUp(self):
        global sharedList
        global sharedListLock
        global barrier
        sharedList = []
        sharedListLock = Semaphore(0)
    
    def test_streaming(self):
        global sharedList
        global sharedListLock
        global hostFakeGlobalBuffer
        global portFakeGlobalBuffer
        # Start fake buffer
        thread.start_new_thread(fakeGlobalBuffer, (portFakeGlobalBuffer,))
        # Create Stop Event
        stopEvent = Event()
        # Create objects for streaming
        search = Search(1, selectRandomTopic())
        api = createApiStreaming()
        buffer_ = bc.Buffer(10, 10, hostFakeGlobalBuffer, portFakeGlobalBuffer)
        # Start thread for streaming
        thread.start_new_thread(streaming, (search, api, buffer_, stopEvent, barrier,))
        # Wait for data in sharedList
        sharedListLock.acquire()
        # send stop to mainStreaming thread
        stopEvent.set()
        barrier.wait()
        self.assertGreater(len(sharedList[0].getList()), 0) # tweets
        self.assertGreater(len(sharedList[1].getList()), 0) # users
        self.assertGreater(len(sharedList[2].getList()), 0) # tweetStreaming

    def test_searchFuture(self):
        global sharedList
        global sharedListLock
        global hostFakeGlobalBuffer
        global portFakeGlobalBuffer
        # Start fake buffer
        thread.start_new_thread(fakeGlobalBuffer, (portFakeGlobalBuffer,))
        # Create Stop Event
        stopEvent = Event()
        # Create objects for streaming
        search = Search(1, selectRandomTopic())
        api = createApiSearch()
        buffer_ = bc.Buffer(10, 10, hostFakeGlobalBuffer, portFakeGlobalBuffer)
        # Start thread for streaming
        thread.start_new_thread(searchFuture, (search, api, buffer_, stopEvent, barrier,))
        # Wait for data in sharedList
        sharedListLock.acquire()
        # send stop to mainStreaming thread
        stopEvent.set()
        barrier.wait()
        self.assertGreater(len(sharedList[0].getList()), 0) # tweets
        self.assertGreater(len(sharedList[1].getList()), 0) # users
        self.assertGreater(len(sharedList[3].getList()), 0) # tweetSearch

    def test_searchHistorical(self):
        global sharedList
        global sharedListLock
        global hostFakeGlobalBuffer
        global portFakeGlobalBuffer
        # Start fake buffer
        thread.start_new_thread(fakeGlobalBuffer, (portFakeGlobalBuffer,))
        # Create Stop Event
        stopEvent = Event()
        # Create objects for streaming
        search = Search(1, selectRandomTopic())
        api = createApiSearch()
        buffer_ = bc.Buffer(10, 10, hostFakeGlobalBuffer, portFakeGlobalBuffer)
        # Start thread for streaming
        thread.start_new_thread(searchHistorical, (search, api, buffer_, stopEvent, barrier,))
        # Wait for data in sharedList
        sharedListLock.acquire()
        # send stop to mainStreaming thread
        stopEvent.set()
        barrier.wait()
        self.assertGreater(len(sharedList[0].getList()), 0) # tweets
        self.assertGreater(len(sharedList[1].getList()), 0) # users
        self.assertGreater(len(sharedList[3].getList()), 0) # tweetSearch

    def test_signalHandler(self):
        global sharedList
        global sharedListLock
        global hostFakeGlobalBuffer
        global portFakeGlobalBuffer
        # Start fake buffer
        thread.start_new_thread(fakeGlobalBuffer, (portFakeGlobalBuffer,))
        # Create Stop Event
        stopEvent = Event()
        # Create objects for streaming
        search = Search(1, selectRandomTopic())
        api = createApiStreaming()
        buffer_ = bc.Buffer(10, 10, hostFakeGlobalBuffer, portFakeGlobalBuffer)
        # Start thread for streaming
        thread.start_new_thread(streaming, (search, api, buffer_, stopEvent, barrier,))
        #delay
        time.sleep(10)
        #Create signal
        fakeSignal(stopEvent, barrier) 
        # Wait for data in sharedList
        sharedListLock.acquire()
