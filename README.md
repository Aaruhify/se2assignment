# se2assignment

Introduction:
	This project contains 3 Modules as Follows


	FileWatcher: This module is responsible to read new files in it's assigned directory with a specific file name regex
		     It also reads new files based on that regex and update's its local DB

	Compressor: This Module is reponsible to read new entries in its assigned DB
		    It also reads new entries and saves after compressing their data in its own local DB

	Validator:  This Module is responsible to read new entries in its assigned compressed DB
		    It validates those data for thos new entries and turns a flag if the data is valid or not.


Tools:
	Python2.7, Python Yaml Library, Other Python basic packages(base64, os etc)


Conecpt:

	Filewatcher comes with 2 different sub modules
	One module is a daemon service for the file watcer which creates a thread for a new assigned directory
	Second moudle is responsible to execute that thread


	Compressor: This moudle has same architecture as filewatcher


Usage:
	Filewatcher:
		To start filewatcher service:
		   $ ./FilewatcherService start

		To assign a directory to a filewatcher thread execute the command in the respective directory:
		   $ ./FilewatcherService init

		To stop filewatcher service:
		   $ ./FilewatcherService stop

	Compressor:
 		To start filewatcher service:
		   $ ./CompressorService start

		To assign a directory to a filewatcher thread execute the command in the respective directory:
		   $ ./CompressorService init

		To stop filewatcher service:
		   $ ./CompressorService stop

	Validator:
		To run Validator:
		   $ ./validator
		For Validator a db path must be shared in validator.yml config file

