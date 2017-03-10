import tweepy
import csv
import time
import datetime
import re
import os
import smtplib
import json
import configparser
import logging
from logging.handlers import RotatingFileHandler
from email.mime.text import MIMEText
from urllib.request import urlopen
from elasticsearch import Elasticsearch

#email stuff
USE_EMAIL = False
EMAIL_SPLIT_VALUE = 4 #if the calculated value is greater than this value it will send an email
EMAIL_TO_ADDRESS = 'localhost@localhost'
EMAIL_FROM_ADDRESS = 'localhost@localhost'
EMAIL_SUBJECT = 'TWITTERBOT HIT:'
EMAIL_HOST = 'localhost'

# access keys
TWITTER_CONSUMER_KEY = ''
TWITTER_CONSUMER_SECRET = ''
TWITTER_ACCESS_TOKEN = ''
TWITTER_ACCESS_SECRET = ''

#elasticsearch junk
USE_ELASTIC = False
ELASTIC_UNAME = 'elastic'
ELASTIC_PASSWORD = 'changme'
ELASTIC_HOST = 'localhost'
ELASTIC_PORT = 9200

#file system
USE_FILE = True
FILE_PATH = 'queued/'

#Log config options
USE_FILELOG = False
LOG_LEVEL = 'INFO'
LOG_FOLDER = 'logs/'

LOGGER = logging.getLogger()
LOGGER.setLevel(LOG_LEVEL)
LOGFORMATTER = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
LOGCONSOLE = logging.StreamHandler(sys.stdout)
LOGCONSOLE.setFormatter(LOGFORMATTER)
LOGGER.addHandler(LOGCONSOLE)

#Watch list system
WATCHLIST_PATH = 'watchlist.csv'
WATCHLIST_TIME_BETWEEN_UPDATES = 3600
WATCHLIST = {}
WATCHLIST_CSV_FIELDS = ['filterword', 'filterregex', 'weight', 'multiplier']

#program variables
CONTINUE_RUNNING = True

#special user ids to watch the default is dumpmon but you can add more if there are ones you are really interested in
SEARCH_USERS = ['1231625892']
OUTPUT_FIELDS = ['calculatedvalue','markers','username','id','text','otherdata', 'created_at']

ES = Elasticsearch()

def getinfovalue(data, multiplier=1):
	'''
	Take in a string and search it for regexes assign weight based on regexes
	'''
	logging.info('Starting calculating infovalues')
	weight = 0
	matches = list()
	for key, val in WATCHLIST.items():
		if val[0].search(data):
			matches.append(key)
			weight += val[1]
			multiplier += val[2]
	calculatedvalue = weight * multiplier
	if calculatedvalue >= EMAIL_SPLIT_VALUE and USE_EMAIL:
		sendmessage(data, matches, calculatedvalue)
	logging.info('finished calculating infovalues')
	return (calculatedvalue, matches)

def sendmessage(dat, keys, calcval):
	'''
	Send an email with the data that got caught 
	'''
	logging.info('sending email to %s', EMAIL_TO_ADDRESS)
	msg = MIMEText(dat)
	msg['Subject'] = EMAIL_SUBJECT + str(calcval) + ' ' + ':'.join(keys)
	msg['From'] = EMAIL_FROM_ADDRESS
	msg['To'] = EMAIL_TO_ADDRESS
	s = smtplib.SMTP(EMAIL_HOST)
	s.send_message(msg)
	s.quit()
	logging.info('email sent to %s', EMAIL_TO_ADDRESS)

def fetchDump(dumpUrl, filedatecode, created_at):
	logging.info('fetching dump from %s', dumpUrl)
	filename = dumpUrl.split('/')[-1:]
	f = urlopen(dumpUrl)
	tempdata = f.read()
	weight = getinfovalue(tempdata.decode('utf-8', errors='ignore'), 2) #dumps get a higher multiplier
	logging.debug(str(weight[0]) + filename[0])
	if weight[0] > 0 and USE_ELASTIC:
		json_data = {}
		json_data['weight'] = weight[0]
		json_data['weight_keys'] = weight[1]
		json_data['text'] = tempdata.decode('utf-8', errors='ignore')
		json_data['dump_url'] = dumpUrl
		json_data['filename'] = filename[0]
		json_data['created_at'] = created_at
		json_data['user'] = {'name':'dumpmon'}
		ES.index(index="twitter", doc_type="dump", body=json.dumps(json_data))
	fh = open(FILE_PATH + filedatecode + '/' + str(weight[0]) + '_'.join(weight[1]) + '-' + filename[0], "wb")
	fh.write(tempdata)
	fh.close()
	logging.info('finished fetching dump from %s', dumpUrl)
	return

class StreamListener(tweepy.StreamListener):
	'''
	listener for tweepy stream
	'''
	def on_status(self, status):
		filedatecode = datetime.date.today().strftime("%Y%m%d")
		if not os.path.exists("queued/" + filedatecode + '/'):
			os.makedirs("queued/" + filedatecode + '/')
		if status.user.id == 1231625892:
			url = status.text.split(' ')[0]
			fetchDump(url, filedatecode, status._json['created_at'])
		else:
			otheroutput = ''
			if status.entities:
				for url in status.entities.get('urls'):
					if url.get('expanded_url'):
						otheroutput += url.get('expanded_url')
			
			outputdict = {}
			outputdict['username'] = status.user.screen_name
			outputdict['id'] = status.id_str
			outputdict['text'] = status.text.replace('\r', '').replace('\n', '')
			outputdict['otherdata'] = otheroutput.replace('\r', '').replace('\n', '')
			tempvalues = getinfovalue(str(outputdict))
			outputdict['calculatedvalue'] = str(tempvalues[0])
			outputdict['markers'] = ':'.join(tempvalues[1])
			outputdict['created_at'] = status.created_at
			if (tempvalues[0] > 0):
				if USE_ELASTIC:
					json_data = status._json
					json_data['weight'] = tempvalues[0]
					json_data['weight_keys'] = tempvalues[1]
					ES.index(index="twitter", doc_type="tweet", body=json_data)
				logging.debug(str(tempvalues[0]) + " " + ':'.join(tempvalues[1]))
				if not os.path.exists(FILE_PATH + filedatecode + '/' + "tweetlog.csv"):
					with open(FILE_PATH + filedatecode + '/' + "tweetlog.csv", 'w', encoding='UTF-8') as fh:
						writer = csv.DictWriter(fh, fieldnames=OUTPUT_FIELDS, lineterminator='\n')
						writer.writeheader()
				with open(FILE_PATH + filedatecode + '/' + "tweetlog.csv", "a", encoding='UTF-8') as fh:
					writer = csv.DictWriter(fh, fieldnames=OUTPUT_FIELDS, lineterminator='\n', escapechar='\\')
					writer.writerow(outputdict)
			
		
	def on_error(self, status_code):
		if status_code == 420:
			#returning False in on_data disconnects the stream
			logging.exception('Being Rate limited stopping the listener')
			return False
		else:
			logging.exception('%s was set by twitter something is wrong', status_code)
			return False

def reloadwatchlist():
	'''
	Reloads the watchlist with the latest data
	'''
	logging.info('started parsing watchlist file %s', WATCHLIST_PATH)
	global WATCHLIST
	listchanged = False
	keywordslist = list()
	with open(WATCHLIST_PATH) as csvfile:
		reader = csv.DictReader(csvfile, fieldnames=WATCHLIST_CSV_FIELDS)
		for row in reader:
			if row['filterword'] == 'filterword':
				continue
			keywordslist.append(row['filterword'])
			if row['filterword'] in WATCHLIST:
				if not (WATCHLIST[row['filterword']][0].pattern == row['filterregex'] and 
						WATCHLIST[row['filterword']][1] == float(row['weight']) and
						WATCHLIST[row['filterword']][2] == float(row['multiplier'])):
					listchanged = True
					WATCHLIST[row['filterword']] = [re.compile(row['filterregex'], re.IGNORECASE), float(row['weight']), float(row['multiplier'])]
			else:
				listchanged = True
				WATCHLIST[row['filterword']] = [re.compile(row['filterregex'], re.IGNORECASE), float(row['weight']), float(row['multiplier'])]
	#clear out any watchlist elements that are not in our list
	removelist = list()
	for key in WATCHLIST.keys():
		if not key in keywordslist:
			removelist.append(key)
	for key in removelist:
		del WATCHLIST[key]
		listchanged = True
	logging.info('finished parsing watchlist file %s', WATCHLIST_PATH)
	return listchanged


def parseconfig(filename='config.ini'):
	'''
	Parses the config into its global variables
	'''
	logging.info('starting parsing config file %s', filename)
	global TWITTER_ACCESS_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET
	global USE_EMAIL, EMAIL_FROM_ADDRESS, EMAIL_HOST, EMAIL_SPLIT_VALUE, EMAIL_SUBJECT, EMAIL_TO_ADDRESS
	global USE_ELASTIC, ELASTIC_HOST, ELASTIC_PASSWORD, ELASTIC_PORT, ELASTIC_UNAME
	global USE_FILE, FILE_PATH
	global WATCHLIST_PATH, WATCHLIST_TIME_BETWEEN_UPDATES
	global USE_FILELOG, LOG_LEVEL, LOG_FOLDER
	config = configparser.ConfigParser()
	config.read(filename)
	
	logconfig = config['logging']
	if logconfig:
		USE_FILELOG = logconfig.getboolean('USE_FILELOG', USE_FILELOG)
		LOG_LEVEL = logconfig.get('LOG_LEVEL', LOG_LEVEL)
		LOG_FOLDER = logconfig.get('LOG_FOLDER', LOG_FOLDER)

	twitterconfig = config['twitter']
	if twitterconfig:
		TWITTER_CONSUMER_KEY = twitterconfig.get('CONSUMER_KEY')
		TWITTER_CONSUMER_SECRET = twitterconfig.get('CONSUMER_SECRET')
		TWITTER_ACCESS_TOKEN = twitterconfig.get('ACCESS_TOKEN')
		TWITTER_ACCESS_SECRET = twitterconfig.get('ACCESS_SECRET')

	watchconfig = config['watchlist']
	if watchconfig:
		WATCHLIST_PATH = watchconfig.get('WATCHLIST_PATH', WATCHLIST_PATH)
		WATCHLIST_TIME_BETWEEN_UPDATES = watchconfig.getint('WATCHLIST_TIME_BETWEEN_UPDATES', WATCHLIST_TIME_BETWEEN_UPDATES)

	emailconfig = config['email']
	if emailconfig:
		USE_EMAIL = emailconfig.getboolean('USE_EMAIL', USE_EMAIL)
		EMAIL_SPLIT_VALUE = emailconfig.getint('SPLIT_VALUE', EMAIL_SPLIT_VALUE)
		EMAIL_TO_ADDRESS = emailconfig.get('TO_ADDRESS', EMAIL_TO_ADDRESS)
		EMAIL_FROM_ADDRESS = emailconfig.get('FROM_ADDRESS', EMAIL_FROM_ADDRESS)
		EMAIL_SUBJECT = emailconfig.get('SUBJECT', EMAIL_SUBJECT)
		EMAIL_HOST = emailconfig.get('HOST', EMAIL_HOST)

	elasticconfig = config['elastic']
	if elasticconfig:
		USE_ELASTIC = elasticconfig.getboolean('USE_ELASTIC', USE_ELASTIC)
		ELASTIC_UNAME = elasticconfig.get('UNAME', ELASTIC_UNAME)
		ELASTIC_PASSWORD = elasticconfig.get('PASSWORD', ELASTIC_PASSWORD)
		ELASTIC_HOST = elasticconfig.get('HOST', ELASTIC_HOST)
		ELASTIC_PORT = elasticconfig.getint('PORT', ELASTIC_PORT)

	fileconfig = config['file']
	if fileconfig:
		USE_FILE = elasticconfig.getboolean('USE_FILE', USE_FILE)
		FILE_PATH = elasticconfig.get('PATH', FILE_PATH)
	
	logging.info('finished parsing config file %s', filename)

def main():
	global ES, CONTINUE_RUNNING
	logging.info('System is starting up')
	#parse the config file so we can set everything up
	parseconfig()
	
	#setup the file logger if we are using it and update the loglevel
	if USE_FILELOG:
		logfilehandler = RotatingFileHandler(LOG_FOLDER, backupCount=3, maxBytes=1000)
		logfilehandler.setFormatter(LOGFORMATTER)
		LOGGER.addHandler(logfilehandler)
	logging.critical('Setting the logging level to %s', LOG_LEVEL)
	LOGGER.setLevel(LOG_LEVEL)

	# setup api and authenticate
	logging.info('Creating the tweepy auth info')
	auth = tweepy.OAuthHandler(TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET)
	auth.set_access_token(TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET)
	api = tweepy.API(auth)
	
	#setup elasticsearch connection
	if USE_ELASTIC:
		logging.info('Creating the connection to the elasticsearch')
		ES = Elasticsearch(hosts=[{'host': ELASTIC_HOST, 'port': ELASTIC_PORT}],http_auth=(ELASTIC_UNAME, ELASTIC_PASSWORD))
		
	while CONTINUE_RUNNING:
		logging.info('Creating the stream listener')
		streamlistener = StreamListener()
		mystream = tweepy.Stream(auth=api.auth, listener=streamlistener)
		try:
			#reload the watchlist and if it returns true start/restart the stream filter
			if reloadwatchlist():
				if mystream.running:
					logging.critical('shutting down stream to reload watch list')
					mystream.disconnect()
				logging.critical('starting the stream filter with %s', WATCHLIST.keys())
				mystream.filter(follow=SEARCH_USERS, track=list(WATCHLIST.keys()), async=True)
			logging.info('waiting %s to check file again', WATCHLIST_TIME_BETWEEN_UPDATES)
			time.sleep(WATCHLIST_TIME_BETWEEN_UPDATES)
		except KeyboardInterrupt:
			logging.exception('keyboard interrupt happened shutting everything down')
			CONTINUE_RUNNING = False
			mystream.disconnect()
		except Exception as ex:
			logging.exception('there was an issue waiting 10 minutes before trying again' + str(ex))
			time.sleep(600)
			mystream.disconnect()

if __name__ == "__main__":
	main()
#__EOF__
