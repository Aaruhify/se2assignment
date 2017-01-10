# Imports start

import os, sys, logging, glob, time, traceback, json
from optparse import OptionParser
import sqlite3
import utils
from FileWatcherConfig import fwconfig

tconfig = fwconfig["watcher"]

defaultDBName = tconfig["dbname"]
defaultDelay = tconfig["sleepDuration"]
defaultConfig = {
    "delay": 60
}
configDirectory = tconfig["configDir"]


fileCreateTable = tconfig["createTableQuery"]

updateQuery = tconfig["updateDataQuery"]

insertQuery = tconfig["insertDataQuery"]

# Imports end


def setupDirectory(path):
    if not os.path.exists(path):
        os.makedirs(path);

class Watchman(object):
    connection = None;
    def __init__(self, config):
        self.directory = config["directory"]
        workingDirectory = os.path.join(config["directory"], configDirectory)
        dbLocation = os.path.join(workingDirectory, defaultDBName)
        setupDirectory(workingDirectory)
        self.connection = utils.setupDBConnection(dbLocation)
        utils.executeDBStatements(self.connection, fileCreateTable);
        self.config = config if config != None else defaultConfig
        self.delay = defaultDelay
 
    def cyclize(self):
        while True:
            try:
                self.initialize()
            except Exception:
                print "Something Failed"
                traceback.print_exc()
            time.sleep(self.delay)
        
    def saveRecord(self, fileName, priority, data):
        query = updateQuery.format(priority, data, 0, fileName)
        count = utils.executeDBStatements(self.connection, query);
        print count;
        if count > 0:
            return;
        query = insertQuery.format(fileName, data, priority, 0)
        print query;
        utils.executeDBStatements(self.connection, query);
            
    def initialize(self):
        os.chdir(self.directory)
        for iFile in glob.glob(".*.json.*[0-9]"):
            fileName = utils.getFileName(iFile, "json")
            priority = utils.getFilePriority(iFile)
            os.rename(iFile, iFile+".processed")
            if not os.path.isfile(fileName):
                print "%s doesn't exist" % fileName
                continue;
            fileBytes = open(fileName, "rb")
            data = fileBytes.read();
            fileBytes.close()
 #           data = json.loads(data)
#            data = json.dumps(data)
            print data
            self.saveRecord(fileName, priority, data)
    
