import sys
import time, hashlib
import thread, os

from daemon import Daemon
from filewatcher import Watchman
from FileWatchManager import WatchManager, WatchClient

import threading

pidFile = "/tmp/filewatch-daemon.pid"




def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

if __name__ == "__main__":
    daemon = WatchManager(pidFile)
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            daemon.start()
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            daemon.restart()
        elif 'init' == sys.argv[1]:
            WatchClient().appendDir(os.getcwd())
        else:
            print "Unknown command"
            sys.exit(2)
        sys.exit(0)
    else:
        print "usage: %s start|stop|restart|init" % sys.argv[0]
        sys.exit(2)
