def test_create_mock_nodes():
	o1={'id':23, 'id_type':'user', 'request_type':'default', 'bio': 'hacker'}
	o2={'id':12, 'id_type': 'startup', 'request_type': 'default', 'pitch': 'make impact'}

def test_create_mock_edges():
	e1={'id':14, 'id_type':'user', 'request_type':'roles', 'startup':{'id':12}, 'tagged':{'type':'User', 'id':23}, 'role':'employee'}
	e2={'id':23, 'id_type': 'startup', 'request_type':'roles', 'startup':{'id':42, 'name':'conviva', 'pitch': 'CDNs'}, 'tagged':{'type':'Startup', 'id':12}, 'role':'customer'}

def test_create_mock_urlresponses():
	u1={'id':12, 'id_type':'user', 'request_type':'default', 'name':'flappy bird', 'bio': 'gamer', 'url':'https://api.mockangel.co/1/users/12'}
	u2={'id':12, 'id_type':'startup', 'request_type':'default', 'name':'viz media', 'pitch': 'different', 'url':'https://api.mockangel.co/1/startups/12'}
	u3={'id_type':'user', 'request_type':'roles', 'per_page':50, 'page':1, 'last_page':10, 'total':495, 'url':'https://api.mockangel.co/1/users/23/roles/', 'startup_roles':[{'id':213, 'role':'board_member', 'startup': {'id':6702, 'name':'mockangellist', 'pitch':'help others'}, 'tagged':{'type':'User', 'id':23, 'name': 'flappy bird', 'bio':'bird'}}, {'id':13, 'role':'past_investor', 'startup':{'id':12,'name':'boo','pitch':'scare people'}, 'tagged':{'id':23, 'type': 'User', 'name': 'yo mama', 'bio':'so fat'}}]}
	u4={'id':12, 'id_type':'user', 'request_type':'default', 'name':'flappy bird', 'bio': 'gamer', 'url':'https://api.mockangel.co/1/users/12'}

def test_crawler_initialize():
	c = Crawler('urllist.txt', db_name='mock_angeldb', urlport='1234', dataport='1235')

def test_crawler_store(crwl):
	# first time store
	crwl._angel_store_obj(o1)
	crwl._angel_store_obj(o2)
	# try storing again
	crwl._angel_store_obj(o1)
	crwl._angel_store_obj(o2)
	# first time edge store
	crwl._angel_store_obj(e1)
	# try storing edge again
	crwl._angel_store_obj(e1)
	# process url response
	crwl.process_url(u3)