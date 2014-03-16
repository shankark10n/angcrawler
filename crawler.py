import zmq
import random
import sys
import time
import pymongo as pm
import json
import logging
import argparse

def initialize_crawler():
	context = zmq.Context()
	zmq_socket = context.socket(zmq.PUSH)
	zmq_socket.bind('tcp://127.0.0.1:5557')
	print 'Initialized crawler socket on port 5557'
	data_socket = context.socket(zmq.PULL)
	data_socket.bind('tcp://127.0.0.1:5558')
	print 'Initialized data socket on port 5558'
	return zmq_socket, data_socket

def push_num(num, zmq_socket):
	msg = {'num': num}
	zmq_socket.send_json(msg)
	print 'Sending msg: %d to crawler' % num

def process_data(data_socket, zmq_socket):
	data = data_socket.recv_json()
	nums = [int(s) for s in data['series'].split(',')]
	for n in nums:
		push_num(n, zmq_socket)
		time.sleep(1)

class Crawler:
	"""Class for the main crawler object. Schedules URLs for Fetcher object and
	processes responses to write to a database.
	@param urlport
	@param dataport
	"""
	def __init__(self, seed_file, db_name = 'angeldb', sent_urls='sent_urls', crawled_urls='crawled_urls', urlport='5557', dataport='5558'):
		self.dbconn = pm.Connection()
		self.dbhandle = self.dbconn[db_name]
		self.sent_urls = sent_urls
		self.crawled_urls = crawled_urls
		self.seed_file = seed_file
		self.sleep_times = [1, 2, 4, 8]
		self.num_sent_urls = 0
		self.num_crawled_urls = 0

		# logging
		self.logger = logging.getLogger('crawler')
		self.logger.setLevel(logging.INFO)
		ch = logging.StreamHandler()
		fh = logging.FileHandler('/home/shankar/tmp/crawler.log')
		ch.setLevel(logging.WARNING)
		fh.setLevel(logging.DEBUG)
		formatter = logging.Formatter("%(asctime)s - [%(name)s.%(levelname)s]: %(message)s")
		ch.setFormatter(formatter)
		fh.setFormatter(formatter)
		self.logger.addHandler(ch)
		self.logger.addHandler(fh)
		self.logger.info('{init} Initialized crawler.')

		# sockets
		self.context = zmq.Context()
		self.pusher = self.context.socket(zmq.PUSH)
		self.puller = self.context.socket(zmq.PULL)
		self.pusher.bind('tcp://127.0.0.1:%s' % urlport)
		self.puller.bind('tcp://127.0.0.1:%s' % dataport)
		self.logger.info('{crawl} Bound to urlport: %s and dataport: %s' % (urlport, dataport))

		# populate from seed file
		url_objs = [json.loads(u.strip()) for u in open(self.seed_file).readlines()]
		for url_obj in url_objs:
			self.push_url(url_obj)
			self.num_sent_urls += 1

	def is_success(self, obj):
		'''Checks for a key called error_type and if it's set to SUCCESS'''
		if obj.has_key('error_type'):
			return (obj['error_type'] == 'SUCCESS')
		else:
			return False

	def is_store_error(self, obj):
		'''Checks for a key called error_type and if it's set to STORE_FAIL'''
		if obj.has_key('error_type'):
			return (obj['error_type'] == 'STORE_FAIL')
		else:
			return False

	def is_quota_exceeded(self, obj):
		'''Checks for a key called error_type and if it's set to QUOTA_EXCEEDED'''
		if obj.has_key('error_type'):
			return obj['error_type'] == 'QUOTA_EXCEEDED'
		else:
			return False

	def is_success(self, obj):
		'''Checks for a key called error_type and if it's set to SUCCESS'''
		if obj.has_key('error_type'):
			return (obj['error_type'] == 'SUCCESS')
		else:
			return False

	def push_url(self, url_obj):
		try:
			# url_mesg = {'url': url_obj['url'], 'id_type': url_obj['id_type'],\
			 # 'request_type': url_obj['request_type']}
			self.pusher.send_json(url_obj)
			self.logger.debug('{push_url} Pushing request for url: %s' % url_obj['url'])
		except KeyboardInterrupt:
			self.logger.warn('{push_url} User abort requested. Exiting.')
			raise
		except:
			self.logger.error('{push_url} Unknown error Pushing %s to socket: %s' %\
			 (url_obj['url'], sys.exc_info()[0]))

	def receive_url(self):
		'''Listens on fetcher's dataport and retrieves URL data. Returns an 
		object based on how the fetcher fared.
		'''
		# url_data = {}
		# url_payload = self.puller.recv_json()
		# if (self.is_success(url_payload)):
		# 	url_data['error_type'] = url_payload['error_type']
		return self.puller.recv_json()

	def recrawl(self, n=1):
		'''Jumpstart the crawl process if there are sent urls that haven't been
		crawled successfully. Compares urls in the sent_urls that aren't in the
		crawled_urls and re-injects them.

		@param n how many URLs to re-inject.
		'''
		crawled_urls_set = set([i['url'] for i in self.dbhandle[self.crawled_urls].find()])
		sent_urls_set = set([i['url'] for i in self.dbhandle[self.sent_urls].find()])
		sent_not_crawled = list(sent_urls_set.difference(crawled_urls_set))
		self.logger.info('{recrawl} Initiating recrawl of %d random URLs from sent_urls' % n)
		for i in random.sample(sent_not_crawled, n):
			obj = {}
			obj['url'] = i
			obj['id_type'] = i.split('/')[4][:-1] # 'user' or 'startup'
			if i.split('/')[-1] != 'roles':
				self.logger.warn('{recrawl} Incorrect URL to crawl: %s' % i)
			else:
				obj['request_type'] = 'roles'
				self.push_url(obj)


	def randomized_sleep(self, sleep_times=None, sleep_file=None):
		'''Sleeps for a random time chosen from sleep_times'''
		self.logger.debug('{randomized_sleep} Sleeping for a random time interval.')
		if (sleep_file is None) and (sleep_times is None):
			sleep_times = self.sleep_times
		elif sleep_file is not None:
			fp = open(sleep_file)
			sleep_obj = json.load(fp)
			if not(sleep_obj.has_key('sleep_times')):
				raise KeyError('{randomized_sleep} sleep_times key not found in %s' % sleep_file)
			sleep_times = sleep_obj['sleep_times']
		time.sleep(random.sample([float(i) for i in sleep_times], 1)[0])

	def _angel_mark_url(self, url_obj, urldb):
		url = {}
		if not(url_obj.has_key('url')):
			raise KeyError('url key not found in url_obj')
		else:
			# url['url'] = url_obj['url'].split('?')[0] #self.get_url(url_obj)
			# if url_obj.has_key('page_number'):
			# 	# url['page_number'] = url_obj['page_number']
			# 	url['url'] = '%s?page=%s' % (url['url'], url_obj['page_number'])
			# 	self.logger.info('{_angel_mark_url} marking url %s on %s'\
			# 	 % (url_obj['url'], urldb))
			# else:
			# 	# url['page_number'] = ''
			# 	url['url']
			url['url'] = url_obj['url']
			if (self.dbhandle[urldb].find(url).count() > 0):
				self.logger.debug('{_angel_mark_url} %s already present in %s'\
				 % (url['url'], urldb))
				return True
			else:
				self.logger.debug('{_angel_mark_url} %s not present in %s. returning False.'\
				 % (url['url'], urldb))
				self.dbhandle[urldb].insert(url)
				return False

	def get_url(self, url_obj):
		'''Returns the url from an object.'''
		self._angel_get_url(url_obj)

	def mark_sent_url(self, url_obj):
		'''Checks if the url in url_obj has been sent before. If sent, 
		returns True; else inserts url and returns False'''
		return self._angel_mark_sent_url(url_obj)

	def _angel_mark_sent_url(self, url_obj):
		return self._angel_mark_url(url_obj, 'sent_urls')

	def _angel_get_request_type(self, url_response):
		if not(url_response.has_key('request_type')):
			raise KeyError('No request_type key found in url_response.')
		else:
			return url_response['request_type']

	def _angel_get_id_type(self, url_response):
		if not(url_response.has_key('id_type')):
			raise KeyError('No id_type key found in url_response.')
		else:
			return url_response['id_type']

	def mock_process_url(self, url_response):
		'''Strips apart the json object and returns some of the Angel data'''
		if ((self._angel_get_request_type(url_response) == 'default')\
			& (self._angel_get_id_type(url_response) == 'user')):
			yield 'Name: %s, Bio: %s' % (url_response['name'], url_response['bio'])
		elif ((self._angel_get_request_type(url_response) == 'default')\
			& (self._angel_get_id_type(url_response) == 'startup')):
			yield 'Name: %s, Quality: %d' % (url_response['name'], url_response['quality'])
		elif ((self._angel_get_request_type(url_response) == 'roles')\
			& (self._angel_get_id_type(url_response) == 'user')):
			yield 'Role: %s, Startup: %s' % (url_response['startup_roles'][0]['role'],\
											url_response['startup_roles'][0]['startup']['name'])
		else:
			yield 'Role: %s, User: %s' % (url_response['startup_roles'][0]['role'],\
										url_response['startup_roles'][0]['tagged']['name'])

	def _angel_id_exists(self, url_response):
		'''Checks if the id given in url_response exists in the hashtable.
		Needs to qualify id by id_type (user/startup)
		'''
		raise NotImplementedError('_angel_id_exists')

	def _angel_node_present(self, obj):
		# [TODO] lookup quick index to see if node already present
		return (self.dbhandle['nodes'].find({'id': obj['id'],\
									 'id_type': obj['id_type']}).count() > 0)

	def _angel_edge_present(self, obj):
		# [TODO] lookup quick index to see if edge already present
		return (self.dbhandle['edges'].find({'id': obj['id']}).count() > 0)

	def _angel_store_node_obj(self, obj):
		self.dbhandle['nodes'].insert(obj)
		self.logger.info('{_angel_store_node_obj} stored node %s' % obj['id'])

	def _angel_store_edge_obj(self, obj):
		edge_obj = {}
		edge_obj['id'] = obj['id']
		edge_obj['crawled_on'] = obj['crawled_on']
		# convention:
		#  if tagged is user, then edge order is (startup, user)
		#  if tagged is startup, then edge order is (startup1, startup2) where 
		#   startup1.id < startup2.id
		if (obj['tagged']['type'] == 'User'):
			edge_obj['v1'] = '%s:startup' % obj['startup']['id']
			edge_obj['v2'] = '%s:user' % obj['tagged']['id']
		elif obj['startup']['id'] < obj['tagged']['id']:
			edge_obj['v1'] = '%s:startup' % obj['startup']['id']
			edge_obj['v2'] = '%s:startup' % obj['tagged']['id']
		else:
			edge_obj['v1'] = '%s:startup' % obj['tagged']['id']
			edge_obj['v2'] = '%s:startup' % obj['startup']['id']
		edge_obj['role'] = obj['role']
		self.dbhandle['edges'].insert(edge_obj)
		self.logger.info('{_angel_store_edge_obj} stored edge (%s, %s, %s)'\
						 % (edge_obj['v1'], edge_obj['v2'], edge_obj['role']))
		# if nodes not present in node collection, store in node collection
		node_obj1 = obj['startup']
		node_obj1['id_type'] = 'startup'
		node_obj1['request_type'] = 'default'
		if not(self._angel_node_present(node_obj1)):
			self._angel_store_node_obj(node_obj1)
		node_obj2 = obj['tagged']
		if node_obj2['type'] == 'User':
			node_obj2['id_type'] = 'user'
		else:
			node_obj2['id_type'] = 'startup'
		node_obj2['request_type'] = 'default'
		if not(self._angel_node_present(node_obj2)):
			self._angel_store_node_obj(node_obj2)

	def _angel_store_obj(self, url_obj):
		'''Stores the object in the data store. Needs to check on what type of
		object this is: node or edge, user or startup. If edge, may still be 
		between new nodes, so need to add nodes as well after adding edge.
		'''
		obj = url_obj
		obj['crawled_on'] = int(time.time())
		try:
			if self._angel_get_request_type(obj) == 'default':
				# store in node collection
				if not(self._angel_node_present(obj)):
					self._angel_store_node_obj(obj)
				else:
					# [TODO] update if more recent version available with different 
					# fingerprint
					self.logger.debug('{_angel_store_obj} node id: %s already exists'\
						% obj['id'])
			else:
				# store in edge collection
				if not(self._angel_edge_present(obj)):
					self._angel_store_edge_obj(obj)
				else:
					# [TODO] update if more recent version available with different 
					# fingerprint
					self.logger.debug('{_angel_edge_present} edge id: %s already exists'\
						% obj['id'])
			return {'error_type': 'SUCCESS'}
		except:
			self.logger.error('{_angel_store_obj}: Unknown error storing object %s' % str(url_obj))
			return {'error_type': 'STORE_FAIL'}

		# raise NotImplementedError('_angel_store_obj')

	# def _angel_process_url_id(self, url_response):
	# 	'''Processes the id part of the url_response. If id doesn't exist in
	# 	hashtable, then inserts into hashtable and data store.
	# 	'''
	# 	if not(self._angel_id_exists(url_response)):
	# 		self._angel_store_obj(url_response)

	# def _angel_process_url_role(self, role_obj):
	# 	'''Processes a given 'role' in the role_obj. If role doesn't exist
	# 	in hashtable, then inserts into hashtable and data store. If tagged
	# 	id doesn't exist in the hashtable, then adds  to list of new ids to
	# 	crawl. Yields the list of new ids to crawl.
	# 	'''
	# 	raise NotImplementedError('_angel_process_url_role')

	def _angel_get_firstlast_page(self, url_response):
		'''Checks if the url_response object is paginated and if it's the 
		first page.'''
		if (url_response.has_key('page') and url_response.has_key('last_page')):
			return int(url_response['page']), int(url_response['last_page'])
		else:
			if url_response.has_key('url'):
				raise KeyError('page or last_page keys not found in object for %s'\
				 % url_response['url'])
			else:
				raise KeyError('page, last_page, url keys not found in url_response')

	def _angel_get_url(self, url_response):
		try:
			if url_response.has_key('url'):
				# strip params from url
				retval = url_response['url'].split('?')[0]
				self.logger.debug('{_angel_get_url} %s' % str(url_response))
				self.logger.debug('{_angel_get_url} returning %s' % retval)
				return retval
			else:
				raise KeyError('url key not found in url_response obj')
		except AttributeError:
			raise AttributeError('empty url? %s' % str(url_response))


	def _angel_get_startup_roles(self, url_response):
		if url_response.has_key('startup_roles'):
			return url_response['startup_roles']
		else:
			raise KeyError('startup_roles key not found in url_response')

	def _angel_schedule_url_remaining_pages(self, url_response):
		try:
			# only should be tried with roles request_type
			if (self._angel_get_request_type(url_response) == 'roles'):
				# if 1st page and there are more than 1 page push URL requests
				f, l = self._angel_get_firstlast_page(url_response)
				self.logger.debug('{_angel_schedule_url_remaining_pages} %s' % str(url_response['url']))
				self.logger.info('{_angel_schedule_url_remaining_pages} first page: %d, last page: %d' % (f, l))
				if l > f:
					self.logger.debug('{_angel_schedule_url_remaining_pages} %s' % str(url_response['url']))
					for i in xrange(f+1, l+1):
						url_obj = url_response
						# url_obj['page_number'] = str(i)
						# strip out params from url
						url_obj['url'] = '%s?page=%s' % (url_response['url'].split('?')[0],\
														str(i)) #self.get_url(url_response)
						self.logger.debug('{_angel_schedule_url_remaining_pages} attempting to push url: %s'\
						 % url_obj['url'])
						if not(self.mark_sent_url(url_obj)):
							self.push_url(url_obj)
							self.num_sent_urls += 1
			else:
				self.logger.debug('{_angel_schedule_url_remaining_pages} request_type is default. so skipping.')
		except KeyError as e:
			raise KeyError('{_angel_schedule_url_remaining_pages} key error with %s. %s' % (str(url_response), e.message))

	def _angel_schedule_role_url(self, id_type, node_id):
		'''Schedules crawls for roles URLs. If original request_type is default,
		schedule roles for node; if original request_type is roles, schedule 
		roles for both startup and tagged.
		'''
		try:
			new_url_obj = {}
			new_url_obj['id_type'] = id_type
			new_url_obj['request_type'] = 'roles'
			common_endpoint_url = 'https://api.angel.co/1'
			if new_url_obj['id_type'] == 'user':
				new_url_obj['url'] = '/'.join([common_endpoint_url, 'users', str(node_id), 'roles'])
			else:
				new_url_obj['url'] = '/'.join([common_endpoint_url, 'startups', str(node_id), 'roles'])
			if not(self._angel_mark_sent_url(new_url_obj)):
				self.logger.debug('{_angel_schedule_role_url} pushing url to socket: %s' % new_url_obj['url'])
				self.push_url(new_url_obj)
				self.num_sent_urls += 1
		except TypeError:
			raise TypeError('{_angel_schedule_role_url} type error for (id_type: %s, node_id: %s)' % (id_type, node_id))

	def _angel_process_url(self, url_response):
		'''Strips out json obj and returns data as a list of JSON objects. For
		new users/startups that are obtained in url_response, consults a hash 
		table to check if they already exist or if they need to be newly
		fetched. Also schedules new pages.

		Returns (current_url_data list, new_urls list)
		'''
		# first schedule remaining pages
		self._angel_schedule_url_remaining_pages(url_response)
		if (self._angel_get_request_type(url_response) == 'default'):
			# no new URLs to fetch
			yield url_response
			# self._angel_process_url_id(url_response)
		elif (self._angel_get_request_type(url_response) == 'roles'):
			# process role for each item in url_response['startup_roles']
			for role in self._angel_get_startup_roles(url_response):
				wrapper_obj = role
				wrapper_obj['id_type'] = self._angel_get_id_type(url_response)
				wrapper_obj['request_type'] = self._angel_get_request_type(url_response)
				wrapper_obj['url'] = url_response['url'].split('?')[0]
				self.logger.debug('{_angel_process_url} url returned: %s' % wrapper_obj['url'])
				self.logger.debug('{_angel_process_url} yielding %s' % str(wrapper_obj))
				yield wrapper_obj				
		else:
			raise ValueError('Incorrect values found in url_response object for %s'\
			 % url_response['url'])


	def process_url(self, url_response):
		return self._angel_process_url(url_response)

	def mock_store_url(self, url_data):
		print url_data
		return {'error_type': 'SUCCESS'}

	def store_url(self, url_data):
		# return self.mock_store_url(url_data)
		return self._angel_store_obj(url_data)

	def mock_schedule_new_url(self, url_data):
		self.logger.info('Dummy.')

	def schedule_new_url(self, url_data):
		self._angel_schedule_new_url(url_data)

	def _angel_schedule_new_url(self, url_data):
		self.logger.debug('{_angel_schedule_new_url} scheduling new url from %s' % url_data['url'])
		# crawl /[users/startups]/:id/roles for url_data
		if (self._angel_get_request_type(url_data) == 'default'):
			self.logger.debug('{_angel_schedule_new_url} calling _angel_schedule_role_url for node %s' % str(url_data))
			self._angel_schedule_role_url(id_type=self._angel_get_id_type(url_data),\
			 node_id=url_data['id'])
		else:
			self.logger.debug('{_angel_schedule_new_url} calling _angel_schedule_role_url for edge %s' % str(url_data))
			self._angel_schedule_role_url(id_type='startup', node_id=url_data['startup']['id'])
			self._angel_schedule_role_url(id_type=url_data['tagged']['type'].lower(),\
											node_id=url_data['tagged']['id'])
		# crawl /[users/startups]/:id/roles?page=[num] where num > 1 (done in process_url)
		# self._angel_schedule_url_remaining_pages(url_data)

	def mark_crawled_url(self, url_obj):
		'''Checks if the URL has already been crawled. Returns True if so, 
		otherwise marks URL as crawled and returns False.
		'''
		self._angel_mark_crawled_url(url_obj)

	def _angel_mark_crawled_url(self, url_obj):
		self.logger.debug('{_angel_mark_crawled_url} marking crawled url')
		return self._angel_mark_url(url_obj, 'crawled_urls')

	def cleanup(self):
		'''Method for cleaning up after execution/interruption of process.
		Mainly used to report statistics.
		'''
		self.logger.debug('Exiting crawl task.')
		self.logger.info('Number of URLs sent in this session: %d' % self.num_sent_urls)
		self.logger.info('Number of URLs crawled in this session: %d' % self.num_crawled_urls)

	def crawl(self):
		try:
			# receive urls from socket
			self.logger.info('{crawl} Listening on data port for URL data')
			url_response = self.receive_url()
			# process urls
			if (self.is_success(url_response)):
				self.num_crawled_urls += 1
				url_objs = self.process_url(url_response)
				for url_obj in url_objs:
					# store in database
					if self.is_success(self.store_url(url_obj)):
						if not(self.mark_crawled_url(url_obj)):
							self.schedule_new_url(url_obj)
					elif (self.is_store_error(url_response)):
						self.logger.error('Object store error for %s' % str(url_obj))
					else:
						self.push_url(url_obj)
					# check if url 
			elif (self.is_quota_exceeded(url_response)):
				reset_at = float(url_response['reset_at'])
				self.logger.warn('{crawl} Crawler quota exceeded. Sleeping for random intervals till %s'\
				 % time.ctime(reset_at))
				#time.sleep(reset_at-10)
				self.randomized_sleep(sleep_times=[20,40,60])
				self.logger.debug('{crawl} Waking up..')
		except KeyboardInterrupt:
			self.logger.warn('{crawl} User abort requested. Exiting.')
			raise
		except AttributeError as e:
			self.logger.error('{crawl} %s' % e.message)
		except NotImplementedError as e:
			self.logger.error('{crawl} %s not implemented' % e.message)
		except TypeError as e:
			self.logger.error('{crawl} %s' % e.message)
		except KeyError as e:
			self.logger.error(e.message)
		except:
			self.logger.error('{crawl} Unknown error during crawl: %s' % sys.exc_info()[0])

def main():
	crawler = Crawler('urllist.txt', db_name='mock_angeldb')
	parser = argparse.ArgumentParser(description='Crawler process.')
	parser.add_argument('-r', '--recrawl', dest='n', type=int, action='store',\
	 nargs='?', help='Initiate recrawl by injecting N urls into the fetch routine.',\
	 default=0)
	ns = parser.parse_args()
	if ns.n is None:
		# python crawler.py -r
		crawler.recrawl()
	elif ns.n < 0:
		# python crawler.py -r -4
		print '#urls to recrawl is < 0'
	elif ns.n > 0:
		# python crawler.py -r 4
		crawler.recrawl(n=ns.n)
	# python crawler.py
	while True:
		try:
			crawler.crawl()
			crawler.randomized_sleep(sleep_file='crawler.json')
		except KeyboardInterrupt:
			break
		finally:
			crawler.cleanup()

def old_main():
	z, d = initialize_crawler()
	lines = open('urllist.txt').readlines()
	for l in lines:
		push_num(int(l.strip()), z)
	while True:
		process_data(d, z)

if __name__ == '__main__':
	main()
