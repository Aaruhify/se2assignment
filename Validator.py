import os
import utils


# Read Files which haven't been validated yet
readQuery = "SELECT * FROM TEMPTABLE WHERE ISVALID=NULL ORDER BY PRIORITY"


def validatePath(path):
    return os.path.exists(path);

class Validator(object):
    def __init__(self, config):
        self.dbLocation = config["dbpath"]
        if validatePath(path) == False:
            print "Error: Invalid Path"
            return;
        self.connection = utils.setupDBConnection("dbpath")
        
    def readDBData(self):
        return executeDBSelectStatement(self.connection, readQuery);
    
    def initialize(self):
        validateJSON(validateTemplate, jsonData)
