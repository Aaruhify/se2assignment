# Imports start

import os, sys, logging, glob, time
from optparse import OptionParser
import sqlite3
import utils

defaultDBName = "tempdb.db"
defaultDelay = 60
defaultConfig = {
    "delay": 60
}
configDirectory = ".filewatcher"


addressTableCreateQuery = "create table if not exists address( addressid INT PRIMARY KEY, streetaddress TEXT NOT NULL, city TEXT NOT NULL,  state TEXT NOT NULL, postalcode TEXT NOT NULL, personid INT UNIQUE REFERENCES PERSON(PERSONID))"

phoneNumberTableCreateQuery = "create table if not exists phonenumber( phoneid INT PRIMARY KEY, type TEXT, number TEXT UNIQUE, personid INT REFERENCES PERSON(PERSONID))"

personTableCreateQuery = "create table if not exists person(personid INT PRIMARY KEY, firstname text not null,  lastname text not null,  middlename text, age INT CHECK (age > 0 and age <= 100), filename TEXT)"

fileCreateTable = "create table if not exists temptable(filename TEXT PRIMARY KEY, filecontent TEXT, priority INT, compressed BOOLEAN)"

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
        self.delay = config["delay"] if config["delay"] != None else defaultDelay
 
    def cyclize(self):
        while True:
            try:
                self.initialize()
            except Exception:
                print "Something Failed"
            time.sleep(self.delay)
        
    def saveRecord(self, fileName, priority, data):
        query = 'update temptable set priority=%d, filecontent="%s", compressed=%d where filename="%s"' % (priority, data, 0, fileName)
        count = utils.executeDBStatements(self.connection, query);
        print count;
        if count > 0:
            return;
        query = 'insert into temptable values("%s", "%s", "%d", "%d")' % (fileName, data, priority, 0)
        print query;
        utils.executeDBStatements(self.connection, query);
            
    def initialize(self):
        while True:
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
                self.saveRecord(fileName, priority, data)
            time.sleep(self.delay)
    
