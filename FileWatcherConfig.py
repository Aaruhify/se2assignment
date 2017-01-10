import yaml

with open('filewatch.yml', 'r') as f:
    fwconfig = yaml.load(f)


#print fwconfig["manager"]["queueFile"]

