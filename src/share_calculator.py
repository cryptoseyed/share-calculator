import traceback
import sys
import datetime
import time
import json

from operator import itemgetter
from itertools import groupby

import requests

import psycopg2

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from colorama import init as colorama_init, Fore

from settings import SETTING


colorama_init(autoreset=True)


START_HIGHT = 160000
WORKING_HIGHT = 160000
SLEEP_TIME = 0.5
BLOCK_REWARD = 41
POOL_FEE = 0
SG_WALLET_RPC_ADDR = 'localhost:12213'
TG_WALLET_RPC_AUTH = ('test', 'test')
SG_DAEMON_ADDR = 'localhost:12211'


def message(string):
	string = str(string)
	print('[' + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + '] '\
	+ Fore.GREEN + 'Message: ' + Fore.RESET + string)

def error(string):
	string = str(string)
	print('[' + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + '] '\
	+ Fore.RED + 'Error: ' + Fore.RESET + string)

def connection_init():
	conn = psycopg2.connect(\
				user=SETTING["psqlUser"], \
				password=SETTING["psqlPass"], \
				host="localhost", \
				port="5432")
	message('Connection created')

	conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
	cur = conn.cursor()
	message('Cursor created')
	return conn, cur

def database_init(cur, conn):
	cur.execute('DROP SCHEMA IF EXISTS wpv1 cascade')
	conn.commit()
	message('Schema wpv1 droped')

	with open('CREATE_DB.sql', 'r') as my_file:
		cur.execute(my_file.read())
	conn.commit()
	message('Schema wpv1 created')

	with open('DATA.sql', 'r') as my_file:
		cur.execute(my_file.read())
	conn.commit()
	message('Data added')

	cur.execute('SET search_path TO wpv1')
	conn.commit()
	message('Set search_path to wpv1')

def get_block_status(cur, height):
	while True:
		time.sleep(SLEEP_TIME)

		cur.execute('SELECT status FROM mined_blocks WHERE height=%s', (height, ))
		result = cur.fetchone()
		if result is None:
			continue
		else:
			return result[0]

def daemon(s_method, d_params=None):
	d_headers = {'Content-Type': 'application/json'}
	d_daemon_input = {"jsonrpc": "2.0", "id": "0", "method" :  s_method}

	if d_params is not None:
		d_daemon_input['params'] = d_params

	o_rsp = requests.post(\
		'http://' + SG_DAEMON_ADDR + '/json_rpc', \
		data=json.dumps(d_daemon_input), \
		headers=d_headers, \
		timeout=60.0)  #Wallet can be fairly slow for large requests

	if o_rsp.status_code != requests.codes.ok: # pylint: disable=maybe-no-member
		raise RuntimeError('HTTP Request error : ' + o_rsp.reason)

	d_jsn = o_rsp.json()
	if 'error' in d_jsn:
		raise RuntimeError("Wallet error: " + d_jsn['error']['message'])

	return d_jsn['result']

def wallet_rpc(s_method, d_params=None):
	d_headers = {'Content-Type': 'application/json'}
	d_rpc_input = {"jsonrpc": "2.0", "id": "0", "method" :  s_method}

	if d_params is not None:
		d_rpc_input['params'] = d_params

	o_rsp = requests.post(\
		'http://' + SG_WALLET_RPC_ADDR + '/json_rpc', \
		data=json.dumps(d_rpc_input), \
		headers=d_headers, \
		timeout=60.0, #Wallet can be fairly slow for large requests
		auth=requests.auth.HTTPDigestAuth(*TG_WALLET_RPC_AUTH))

	if o_rsp.status_code != requests.codes.ok: # pylint: disable=maybe-no-member
		raise RuntimeError('HTTP Request error : ' + o_rsp.reason)

	d_jsn = o_rsp.json()
	if 'error' in d_jsn:
		raise RuntimeError("Wallet error: " + d_jsn['error']['message'])

	return d_jsn['result']

def valid_shares_between_block(cur, height):
	prev_time = 0
	if height != START_HIGHT:
		cur.execute('SELECT time FROM mined_blocks WHERE height=%s', (height - 1, ))
		prev_time = cur.fetchone()

		prev_time = prev_time[0]

	cur.execute('SELECT time FROM mined_blocks WHERE height=%s', (height, ))
	cure_time = cur.fetchone()[0]

	cur.execute('SELECT * FROM valid_shares WHERE time BETWEEN %s AND %s', \
					(prev_time + 1, cure_time))

	return cur.fetchall()

def get_block_id(cur, height):
	cur.execute('SELECT blk_id from mined_blocks WHERE height=%s', (height, ))
	return cur.fetchone()[0]

def get_user_wallet(cur, uid):
	cur.execute('SELECT wallet FROM users WHERE uid=%s', (uid, ))
	return cur.fetchone()[0]

def record_credit(cur, blk_id, uid, credit):
	cur.execute('INSERT INTO credits (blk_id, uid, amount) VALUES (%s, %s, %s)', \
						(blk_id, uid, credit))

def record_payment(cur, uid, amount, txid, txtime):
	cur.execute('INSERT INTO payments (uid, amount, txid, time) VALUES (%s, %s, %s, %s)', \
						(uid, amount, txid, txtime))

def calculate_credit(cur, height):

	valid_shares = valid_shares_between_block(cur, height)

	valid_shares.sort(key=lambda x: int(x[1]))
	user_total_valid_share_in_block = []
	total_valid_share_in_block = 0

	for elt, items in groupby(valid_shares, itemgetter(1)):
		user_valid_shares_count = 0
		for i in items:
			user_valid_shares_count += int(i[3])
		total_valid_share_in_block += user_valid_shares_count
		user_total_valid_share_in_block.append({'uid': elt, 'valid_shares': user_valid_shares_count})

	blk_id = get_block_id(cur, height)

	destinations = []

	for i, _ in enumerate(user_total_valid_share_in_block):
		user_total_valid_share_in_block[i]['credit'] = \
		user_total_valid_share_in_block[i]['valid_shares'] * \
		(BLOCK_REWARD * (1 - POOL_FEE) / total_valid_share_in_block)

		user_wallet = get_user_wallet(cur, user_total_valid_share_in_block[i]['uid'])

		destinations.append({'amount': int(user_total_valid_share_in_block[i]['credit']), \
							'address': user_wallet})

		record_credit(cur, blk_id, user_total_valid_share_in_block[i]['uid'], \
							user_total_valid_share_in_block[i]['credit'])

	message('Block ' + str(height) + ' valid shares calculated')

	json_data = wallet_rpc('transfer', {'destinations': destinations, 'get_tx_key': True})

	for i in user_total_valid_share_in_block:
		record_payment(cur, i['uid'], int(i['credit']), json_data['tx_hash'], int(time.time()))

	message('Block ' + str(WORKING_HIGHT - 60) + ' payment completed')

def transaction_seen(cur, height):
	cur.execute('UPDATE mined_blocks SET status=2 WHERE height=%s', (height, ))
	message('Block ' + str(height) + ' status updated to 2')

	cur.execute('SELECT status FROM mined_blocks WHERE height=%s', (height - 60, ))
	result = cur.fetchone()

	if result is not None:
		if result[0] == 2:
			update_status(cur, height-60, 3)

def transaction_unlocked(cur, height):
	cur.execute('UPDATE mined_blocks SET status=3 WHERE height=%s', (height, ))
	message('Block ' + str(height) + ' status updated to 3')

def update_status(cur, height, status):
	if status == 2:
		transaction_seen(cur, height)
	elif status == 3:
		transaction_unlocked(cur, height)

		calculate_credit(cur, height)
	else:
		raise RuntimeError('Invalid status code')

try:
	CONN = None
	CURS = None
	CONN, CURS = connection_init()

	database_init(CURS, CONN)

	while True:

		RESULT = get_block_status(CURS, WORKING_HIGHT)

		if RESULT in (0, 1):

			JSON_DATA = daemon('get_block', {'height': WORKING_HIGHT})

			update_status(CURS, WORKING_HIGHT, 2)

		WORKING_HIGHT += 1

except KeyboardInterrupt:
	sys.exit()

except RuntimeError as my_exception:
	error(my_exception)
	traceback.print_exception(*sys.exc_info())
