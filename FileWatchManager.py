import os, threading
import time
import sys
from daemon import Daemon
from filewatcher import Watchman
import utils
from FileWatcherConfig import fwconfig

tconfig = fwconfig["manager"]


queueFile = tconfig["queueFile"]
queueReadDelay = tconfig["sleepDuration"]


threadLock = threading.Lock();
class WatcherThread (threading.Thread):
    def __init__(self, config, name):
        threading.Thread.__init__(self)
        self.name = name
        self.config = config
    def run(self):
        print "Starting " + self.name
        # Get lock to synchronize threads
        threadLock.acquire()
        Watchman(self.config).cyclize();
        # Free lock to release next thread
        threadLock.release()



class WatchClient(object):
    def appendDir(self, directory):
        with open(queueFile, "a") as outFile:
            outFile.write(directory)
            
class WatchManager(Daemon):
    def appendDir(self, directory):
        with open(queueFile, "a") as outFile:
            outFile.write(directory)
            
    def appendDirToStop(self, directory):
        with open(queueFile, "a") as outFile:
            outFile.write(directory + "- stop")
            
    def readListDirs(self,fname):
        if not os.path.isfile(fname) :
            return [];
        with open(fname) as f:
            content = f.read().splitlines()
        with open(fname, "w") as f:
            f.close()    
        return content;
    
    def run(self):
        self.threads = {}
        while True:
            dirs = self.readListDirs(queueFile)
            for directory in dirs:
                conf = utils.watcherConfigWrapper(directory, 3)
                thread1 = WatcherThread(conf, "FileWatchThread")
                thread1.start()
                self.threads[directory] = thread1;
            time.sleep(queueReadDelay);
