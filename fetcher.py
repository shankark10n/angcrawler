import zmq
import random
import sys
import time
import requests
import logging
import json

def crawler_initialize():
	context = zmq.Context()
	crawler_socket = context.socket(zmq.PULL)
	crawler_socket.connect('tcp://127.0.0.1:5557')
	print 'Connecting to crawler on port: 5557. Listening for fetch requests.'
	data_socket = context.socket(zmq.PUSH)
	data_socket.connect('tcp://127.0.0.1:5558')
	print 'Connecting to data bucket on port: 5558.'
	return crawler_socket, data_socket

def num_fetch(zmq_socket, data_socket):
	data = zmq_socket.recv_json()
	num = int(data['num'])
	series = [str(x) for x in random.sample(xrange(10), num)]
	data_socket.send_json({'series': ','.join(series)})
	print 'Sending back %d-tuple: [%s].' % (num, ','.join(series))

def old_main():
	z, d = crawler_initialize()
	while True:
		num_fetch(z, d)

class Fetcher:
	"""Class for the actual URL pulling"""
	def __init__(self, throttle_limit, urlport='5557', dataport='5558', duration=3600):
		'''
		@param throttle_limit number of URLs to retrieve per hour
		@param urlport port at which connect to fetch URLs
		@param dataport port to connect to dump URL data
		@param duration time interval for making calls within quota limit
		'''
		self.throttle_limit = throttle_limit
		self.num_urls_retrieved = 0
		self.num_calls = 0
		self.num_total_calls = 0
		self.bad_requests = 0
		self.start_time = time.time()
		self.hourglass = self.start_time + duration
		self.sleep_times = [0.25, 0.5, 1, 2, 4]
		self.sent_quota_warning = False

		# logging
		self.logger = logging.getLogger('fetcher')
		self.logger.setLevel(logging.INFO)
		ch = logging.StreamHandler()
		fh = logging.FileHandler('/home/shankar/tmp/fetcher.log')
		ch.setLevel(logging.WARNING)
		fh.setLevel(logging.DEBUG)
		formatter = logging.Formatter("%(asctime)s - [%(name)s.%(levelname)s]: %(message)s")
		ch.setFormatter(formatter)
		fh.setFormatter(formatter)
		self.logger.addHandler(ch)
		self.logger.addHandler(fh)
		self.logger.info('{__init__} Initialized fetcher.')

		# sockets
		self.context = zmq.Context()
		self.puller = self.context.socket(zmq.PULL)
		self.pusher = self.context.socket(zmq.PUSH)
		self.puller.connect('tcp://127.0.0.1:%s' % urlport)
		self.pusher.connect('tcp://127.0.0.1:%s' % dataport)
		self.logger.info('{__init__} Connected to urlport: %s and dataport: %s' % (urlport, dataport))

	def clock_reset(self, duration=1800):
		'''Resets the clock for the URL fetcher to respect hourly quotas
		'''
		self.start_time = time.time()
		self.hourglass = self.start_time + duration
		self.logger.info('{clock_reset} Resetting clock. Next reset at %s' % str(self.hourglass))

	def randomized_sleep(self, sleep_times=None):
		'''Sleeps for a random time chosen from sleep_times'''
		self.logger.debug('{randomized_sleep} Sleeping for a random time interval.')
		if sleep_times is None:
			sleep_times = self.sleep_times
		time.sleep(random.sample(sleep_times, 1)[0])

	def request_url(self):
		try:
			'''Executes main logic of requesting the URL.'''
			data = self.puller.recv_json()
			url = data['url']
			id_type = data['id_type']
			request_type = data['request_type']
			# sleep for a random time
			self.randomized_sleep()
			# check if url has page_number specified
			if data.has_key('page_number'):
				page_number = data['page_number']
				retrieve = requests.get(url, params={'page': page_number})
			else:
				retrieve = requests.get(url)
			if (retrieve.status_code == 200):
				self.logger.info('{request_url} Success fetching %s' % retrieve.url)
				retrieved_object = retrieve.json()
				retrieved_object['error_type'] = 'SUCCESS'
				retrieved_object['url'] = retrieve.url
				retrieved_object['id_type'] = id_type
				retrieved_object['request_type'] = request_type
				self.pusher.send_json(retrieved_object)
				self.logger.debug('{request_url} Sending payload to processor')
				self.num_urls_retrieved += 1
			else:
				# report error in getting URL
				self.logger.error('{request_url} Failure fetching %s. %s' %\
				 (retrieve.url, retrieve.json()['error']['message']))
				self.pusher.send_json({'error_type': 'FETCH_FAIL',\
					'url': retrieve.url,
					'message': retrieve.json()['error']['message']})
				self.bad_requests += 1
		except Exception as e:
			self.logger.error('{request_url} Unknown error during request_url: %s' % sys.exc_info()[0])
			self.pusher.send_json({'error_type': 'UNKNOWN_ERROR',\
				'message': e.message})

	def fetch_url(self):
		try:
			if ((time.time() < self.hourglass) & (self.num_calls < self.throttle_limit)):
				# still have time before clock is reset and num_calls is within limits
				self.num_calls += 1
				self.num_total_calls += 1
				self.request_url()
			# elif ((time.time() >= self.hourglass) & (self.num_calls < self.throttle_limit)):
			# 	# past clock but num_calls is within limits
			# 	self.clock_reset()
			# 	self.num_calls = 0
			# 	self.num_total_calls += 1
			# 	self.logger.info('Past the API quota duration. Resetting clock.')
			# 	self.request_url()
			elif ((time.time() < self.hourglass) & (self.num_calls >= self.throttle_limit)):
				# still have time but exceeded number of calls;
				# send one-time error message back to processor & sleep
				self.logger.warn('{fetch_url} Exceeded number of calls possible within period.')
				if not self.sent_quota_warning:
					self.pusher.send_json({'error_type': 'QUOTA_EXCEEDED',\
					 'message': 'Exceeded quota limit.',\
					 'reset_at': str(self.hourglass)})
					self.logger.debug('{fetch_url} Sending backoff message to processor')
					self.sent_quota_warning = True
				self.logger.debug('{fetch_url} Sleeping for random time intervals until reset at %s' % time.ctime(self.hourglass))
				self.randomized_sleep(sleep_times=[60,120])
				self.logger.debug('{fetch_url} Waking up..')
				self.set_throttle_limit()
				# wake up only 10 seconds before reset
				# self.logger.info('{fetch_url} Sleeping till reset at %s' % time.ctime(self.hourglass))
				# time.sleep(self.hourglass)
				# self.logger.info('{fetch_url} Waking up..')
			else:
				# past clock; reset clock, reset num_calls
				self.clock_reset()
				self.num_calls = 0
				self.num_total_calls += 1
				self.logger.info('{fetch_url} Past the API quota duration. Resetting clock.')
				self.request_url()
		except KeyboardInterrupt:
			self.logger.warn('{fetch_url} Received user abort. Exiting.')
			raise
		except:
			self.logger.error('{fetch_url} Unknown error during fetch_url: %s' % sys.exc_info()[0])
			raise

	def set_throttle_limit(self, filename='fetcher.json'):
		fp = open(filename)
		state_obj = json.load(fp)
		if not state_obj.has_key('throttle_limit'):
			raise KeyError('{get_throttle_limit} throttle_limit key not found in %s' % filename)
		self.throttle_limit = int(state_obj['throttle_limit'])
		fp.close()

	def cleanup(self):
		'''Method for cleaning up after execution/interruption of process.
		Mainly used to report statistics.
		'''
		self.logger.debug('{cleanup} Exiting fetch task')
		self.logger.info('{cleanup} Number of calls made since last reset: %d' % self.num_calls)
		self.logger.info('{cleanup} Number of total calls made: %d' % self.num_total_calls)
		self.logger.info('{cleanup} Number of URLs retrieved: %d' % self.num_urls_retrieved)
		self.logger.info('{cleanup} Number of bad requests: %d' % self.bad_requests)

def main():
	fetch = Fetcher(450)
	while True:
		try:
			fetch.fetch_url()
		except KeyboardInterrupt:
			break
		finally:
			fetch.cleanup()

if __name__ == '__main__':
	main()
