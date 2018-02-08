import requests
import time
import urllib.parse
import pymongo
from datetime import datetime

language = 'en'
con = pymongo.MongoClient()
users_collection = con['wikipedia']['en_users']

def str_to_time(time_str):
	return datetime.strptime(time_str, '%Y-%m-%dT%H:%M:%SZ')

def run():
	last_user = None
	mean_time = None
	count = 0

	# very important for replace_one()
	users_collection.create_index([('userid', pymongo.ASCENDING)], unique=True)

	while True:
		# if first iteration, get the user which was updated last
		if last_user == None:
			last_user = list(users_collection.find({}).sort('updated', pymongo.DESCENDING).limit(1))
			if len(last_user) > 0:
				last_user = last_user[0]['name']
			else:
				last_user = ''
			
		time_start = time.time()
		try:
		    url = ('https://{0}.wikipedia.org/w/api.php?action=query&list=allusers&aulimit=500' + \
				    '&auprop=blockinfo|groups|editcount|registration&aufrom={1}&format=json').format(
					    language, urllib.parse.quote_plus(last_user))
		    r = requests.get(url)
		    data = r.json()
		
		    if 'continue' in data:
			    last_user = data['continue']['aufrom']
		    else:
			    last_user = ''
		    print('Last user: ' + last_user)
		
		    # add update time and parse other datetimes
		    for user in data['query']['allusers']:
			    user['updated'] = datetime.now()
			    if 'registration' in user and user['registration']:
				    user['registration'] = str_to_time(user['registration'])
			    if 'blockedtimestamp' in user and user['blockedtimestamp']:
				    user['blockedtimestamp'] = str_to_time(user['blockedtimestamp'])
			    users_collection.replace_one({'userid':user['userid']}, user, True)
		except Exception as e:
			print(e)
			time.sleep(30)

		# calculate average step time
		time_end = time.time()
		req_time = time_end - time_start
		count += 1
		if mean_time == None:
			mean_time = req_time
		else:
			mean_time = mean_time + (req_time - mean_time) / count
		print('Avg batch time: {0} s'.format(mean_time))
		print('Users collected: {0}'.format(users_collection.count()))
		print()

if __name__ == "__main__":
    run()
