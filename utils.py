import sqlite3, os


def watcherConfigWrapper(directory, delay):
    config = {
        "directory": directory,
        "delay": delay
    }
    return config
    

def setupDBConnection(dbLocation):
    conn = sqlite3.connect(dbLocation);
    return conn

def executeDBStatements(connection, query):
    print "Info: Executing Query ", query
    cursor = connection.cursor()
    cursor.execute(query)
    count = cursor.rowcount;
    connection.commit()
    return count

def executeDBSelectStatements(connection, query):
    print "Info: Executing Query ", query
    cursor = connection.cursor()
    cursor.execute(query)
    records = cursor.fetchall();
    return records
    

def getFileName(fileName, extension):
    extension = "." + extension;
    fName = (fileName[1:]).split(extension, 1)[0]
    fName = fName + extension;
    return fName

def getFilePriority(fileName):
    last = fileName.split('.')
    priority = int(last[len(last) - 1])
    return priority
    
