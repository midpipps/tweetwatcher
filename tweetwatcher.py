import tweepy
import csv
import time
import datetime
import re
import os
import smtplib
import json
from email.mime.text import MIMEText
from urllib.request import urlopen
from elasticsearch import Elasticsearch

#email stuff
SPLIT_VALUE = 4 #if the calculated value is greater than this value it will send an email
TO_ADDRESS = 'email'
FROM_ADDRESS = 'email'
SUBJECT = 'TWITTERBOT HIT:'
USE_EMAIL = True
EMAIL_SERVER = 'emailserver'
CONTINUE_RUNNING = True

# access keys
CONSUMER_KEY = ''
CONSUMER_SECRET = ''
ACCESS_TOKEN = ''
ACCESS_SECRET = ''

#elasticsearch junk
ELASTIC_UNAME = ''
ELASTIC_PASSWORD = ''

# setup api and authenticate
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
api = tweepy.API(auth)

#Search regexes are the keywords to search for and the regex with values and multipliers
#example 'word/wordstosearchfor': [re.compile('regexofwordstosearchfor', re.IGNORECASE), weight, multiplier]
#The word is passed to twitter for returning results
#The regex is used to search for the words in the returned tweets
#The weight is how much you care about the word
#The multiplier would be for words that are not nescessarily important but are important if you see them with the other keywords
search_regexes = {
				}
				  
#special user ids to watch the default is dumpmon but you can add more if there are ones you are really interested in
search_users = ['1231625892']


outputfields = ['calculatedvalue','markers','username','id','text','otherdata', 'created_at']

es = Elasticsearch(http_auth=(ELASTIC_UNAME, ELASTIC_PASSWORD))

def getinfovalue(data, multiplier=1):
	'''
	Take in a string and search it for regexes assign weight based on regexes
	'''
	weight = 0
	matches = list()
	for key, val in search_regexes.items():
		if val[0].search(data):
			matches.append(key)
			weight += val[1]
			multiplier += val[2]
	calculatedvalue = weight * multiplier
	if calculatedvalue >= SPLIT_VALUE and USE_EMAIL:
		sendmessage(data, matches, calculatedvalue)
	return (calculatedvalue, matches)

def sendmessage(dat, keys, calcval):
	'''
	Send an email with the data that got caught 
	'''
	msg = MIMEText(dat)
	msg['Subject'] = SUBJECT + str(calcval) + ' ' + ':'.join(keys)
	msg['From'] = FROM_ADDRESS
	msg['To'] = TO_ADDRESS
	s = smtplib.SMTP(EMAIL_SERVER)
	s.send_message(msg)
	s.quit()
	
class DumpMonStreamListener(tweepy.StreamListener):
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
				json_data = status._json
				json_data['weight'] = tempvalues[0]
				json_data['weight_keys'] = tempvalues[1]
				es.index(index="twitter", doc_type="tweet", body=json_data)
				print(str(tempvalues[0]) + " " + ':'.join(tempvalues[1]))
				if not os.path.exists("queued/" + filedatecode + '/' + "tweetlog.csv"):
					with open("queued/" + filedatecode + '/' + "tweetlog.csv", 'w', encoding='UTF-8') as fh:
						writer = csv.DictWriter(fh, fieldnames=outputfields, lineterminator='\n')
						writer.writeheader()
				with open("queued/" + filedatecode + '/' + "tweetlog.csv", "a", encoding='UTF-8') as fh:
					writer = csv.DictWriter(fh, fieldnames=outputfields, lineterminator='\n', escapechar='\\')
					writer.writerow(outputdict)
			
		
	def on_error(self, status_code):
		if status_code == 420:
			#returning False in on_data disconnects the stream
			print('Being Rate limited stopping the listener')
			return False
		else:
			print(status_code)
			return False
		

def fetchDump(dumpUrl, filedatecode, created_at):
	filename = dumpUrl.split('/')[-1:]
	f = urlopen(dumpUrl)
	tempdata = f.read()
	weight = getinfovalue(tempdata.decode('utf-8', errors='ignore'), 2) #dumps get a higher multiplier
	print(str(weight[0]) + filename[0])
	if weight[0] > 0:
		json_data = {}
		json_data['weight'] = weight[0]
		json_data['weight_keys'] = weight[1]
		json_data['text'] = tempdata.decode('utf-8', errors='ignore')
		json_data['dump_url'] = dumpUrl
		json_data['filename'] = filename[0]
		json_data['created_at'] = created_at
		json_data['user'] = {'name':'dumpmon'}
		es.index(index="twitter", doc_type="dump", body=json.dumps(json_data))
	fh = open("queued/%s" % filedatecode + '/' + str(weight[0]) + '_'.join(weight[1]) + '-' + filename[0], "wb")
	fh.write(tempdata)
	fh.close()
	return

	
while CONTINUE_RUNNING:
	streamlistener = DumpMonStreamListener()
	mystream = tweepy.Stream(auth=api.auth, listener=streamlistener)
	try:
		mystream.filter(follow=search_users, track=list(search_regexes.keys()))
	except KeyboardInterrupt:
		print('keyboard interrupt happened')
		CONTINUE_RUNNING = False
		mystream.disconnect()
	except Exception as ex:
		print('there was an issue waiting 10 minutes before trying again' + str(ex))
		time.sleep(600)
		mystream.disconnect()
#__EOF__
