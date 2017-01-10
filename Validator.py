import os
import utils, json, itertools
import re
import base64, time
from ValidatorConfig import vdconfig



vconfig = vdconfig["validator"]

# Read Files which haven't been validated yet
readQuery = vconfig["readDataQuery"]
updateQuery = vconfig["updateDataQuery"]

validatorDB = vconfig["dbpath"]
delayTime = vconfig["sleepDuration"]
validationConfigFile = vconfig["validationConfigPath"]

def validateJSON(conf, data):
    result = True
    mandatory = filter(lambda x : x["mandatory"], conf)
    missingRequiredFields = []
    
    for ob in mandatory:
        if not ob["fieldName"] in data:
            missingRequiredFields.append(ob["fieldName"])
        # if "subfields" in ob:
        #     validateJSON(ob["subfields"], data[ob["fieldName"]])

    if len(missingRequiredFields) > 0:
        print "Error: Missing Required Fields ", missingRequiredFields
        result = False
        
    ## match regexes
    regexFields = filter(lambda x : "regex" in x, conf)
    invalidRegexFields = []
#    print regexFields
    for ob in regexFields:
        if not ob["fieldName"] in data:
            continue;
        resVal = False
        for reg in ob["regex"]:
            value = data[ob["fieldName"]]
            value = str(value)
            #reg = r'^0*(?:[1-9][0-9]?|100)$'
            if re.match(reg, value):
                resVal = True
        if not resVal:
            invalidRegexFields.append(ob["fieldName"])

    if len(invalidRegexFields) > 0:
        print "Error: Invalid Data in fields ", invalidRegexFields
        result = False


    # Test Subfields here!
    subFields = filter(lambda x : "subfields" in x, conf)

    for subField in subFields:
        fieldVal = data[subField["fieldName"]]
        if isinstance(fieldVal, list):
            result = validateJSONLst(subField["subfields"], fieldVal) and result
        else:
            result = validateJSON(subField["subfields"], fieldVal) and result

    return result
    
    

    
        
def validateJSONLst(conf, datas):
    listResult = map(validateJSON, itertools.repeat(conf, len(datas)), datas)
    result = True
    for boolean in listResult:
        result = result and boolean
    return result;


def validatePath(path):
    return os.path.exists(path);


# with open('data.json') as data_file:
#     data = json.load(data_file)
#  #   print data
    
with open(validationConfigFile) as d_f:
    validationConfig = json.load(d_f)
#    print validationConfig



class Validator(object):
    def __init__(self):
        self.connection = utils.setupDBConnection(validatorDB)
        self.delay = delayTime
        
    def readDBData(self):
        print "INFO: Reading Data"
        return utils.executeDBSelectStatements(self.connection, readQuery);

    def writeDBData(self, filename, valid):
        query = updateQuery.format(valid, filename)
        utils.executeDBStatements(self.connection, query)
    
    def initialize(self):
        while True:
            records = self.readDBData()
            for record in records:
                data = base64.b64decode(record[1])
                try:
                    data = json.loads(data)
                except:
                    print "Error: Not a valid JSON Object"
                    self.writeDBData(record[0], 0);
                    continue;
                print "Info: Validating Data"
                valid = validateJSONLst(validationConfig, data["persons"])
                isvalid = 1 if valid else 0;
                self.writeDBData(record[0], isvalid);
            time.sleep(self.delay)
            

Validator().initialize()
