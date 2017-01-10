# Imports start

import os, sys, logging, glob, time, traceback
import base64
from optparse import OptionParser
import sqlite3
import utils
import zlib, hashlib

import sys

reload(sys)
sys.setdefaultencoding('utf8')

defaultDBName = "tempdb.tgz.md5"
defaultDelay = 60
defaultConfig = {
    "delay": 60
}
configDirectory = ".compressor"


addressTableCreateQuery = "create table if not exists address( addressid INT PRIMARY KEY, streetaddress TEXT NOT NULL, city TEXT NOT NULL,  state TEXT NOT NULL, postalcode TEXT NOT NULL, personid INT UNIQUE REFERENCES PERSON(PERSONID))"

phoneNumberTableCreateQuery = "create table if not exists phonenumber( phoneid INT PRIMARY KEY, type TEXT, number TEXT UNIQUE, personid INT REFERENCES PERSON(PERSONID))"

personTableCreateQuery = "create table if not exists person(personid INT PRIMARY KEY, firstname text not null,  lastname text not null,  middlename text, age INT CHECK (age > 0 and age <= 100), filename TEXT)"

fileCreateTable = "create table if not exists temptable(filename TEXT PRIMARY KEY, compressedfilecontent VARBINARY, filehash TEXT, priority INT, isvalid boolean)"

updateQuery = 'update temptable set priority={0}, compressedfilecontent="{1}", filehash="{2}", isvalid={3} where filename="{4}"'


updateQuery2 = 'update temptable set priority=%d, compressedfilecontent="%s", filehash="%s", isvalid="%d" where filename="%s"'

insertQuery = 'insert into temptable values("{0}", "{1}", "{2}", "{3}", {4})'

selectQuery = 'select * from temptable where compressed=0 order by priority'

updateTempQuery = 'update temptable set compressed=%d where filename="%s"'

# Imports end

def md5Hash(data):
    hash_md5 = hashlib.md5()
    hash_md5.update(data)
    return hash_md5.hexdigest()


def setupDirectory(path):
    if not os.path.exists(path):
        os.makedirs(path);

        
class Compressor(object):
    connection = None;
    def validateConfig(self, config):
        readDBLocation = config["dbpath"]        
        if readDBLocation == None or not os.path.exists(readDBLocation):
            print "Input DB Doesn't Exist"
        self.directory = config["directory"]
        workingDirectory = os.path.join(config["directory"], configDirectory)
        dbLocation = os.path.join(workingDirectory, defaultDBName)
        if not os.path.exists(dbLocation):
            print "Compressor Already Initialized in this directory"

        
    def __init__(self, config):
        self.validateConfig(config)
        self.directory = config["directory"]
        workingDirectory = os.path.join(config["directory"], configDirectory)
        dbLocation = os.path.join(workingDirectory, defaultDBName)
        readingDirectory = config["dbpath"]
        setupDirectory(workingDirectory)
        self.connection = utils.setupDBConnection(dbLocation)
        self.readConnection = utils.setupDBConnection(readingDirectory)
        utils.executeDBStatements(self.connection, fileCreateTable);
        self.config = config if config != None else defaultConfig
        self.delay = config["delay"] if config["delay"] != None else defaultDelay
 
    def cyclize(self):
        while True:
            try:
                self.initialize()
            except Exception:
                print "Something Failed"
                traceback.print_exc()
            time.sleep(self.delay)

    def getNewRecords(self):
        records = utils.executeDBSelectStatements(self.readConnection, selectQuery)
        for record in records:
            query = updateTempQuery % (1, record[0])
            count = utils.executeDBStatements(self.readConnection, query)
        return records;
        
    def saveRecord(self, fileName, priority, data):
        fileCheckSum = md5Hash(fileName)
        data = base64.b64encode(data)
        print data;
        query = updateQuery.format(priority, data, fileCheckSum, "null", fileName)
        query.encode('utf8')
        count = utils.executeDBStatements(self.connection, query);
        print count;
        if count > 0:
            return;
        query = insertQuery.format(fileName, data, fileCheckSum, priority, "null")
        utils.executeDBStatements(self.connection, query);
            
    def initialize(self):
        records = self.getNewRecords();
        for record in records:
            fileName = record[0];
            data = record[1];
            priority = record[2]
            self.saveRecord(fileName, priority, data);
        
    
