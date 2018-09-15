import traceback
import sys
import datetime
import time
import json
from os import urandom
from binascii import hexlify

from operator import itemgetter
from itertools import groupby

import requests

import psycopg2

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from colorama import init as colorama_init, Fore

from settings import SETTING


colorama_init(autoreset=True)


SLEEP_TIME = SETTING['SLEEP_TIME']
BLOCK_REWARD = SETTING['BLOCK_REWARD']
POOL_FEE = SETTING['POOL_FEE']
SG_WALLET_RPC_ADDR = SETTING['SG_WALLET_RPC_ADDR_TESTNET']
TG_WALLET_RPC_AUTH = SETTING['TG_WALLET_RPC_AUTH_TESTNET']
SG_DAEMON_ADDR = SETTING['SG_DAEMON_ADDR_TESTNET']
CHANGE_STATUS_TO_SUCCESS_LIMIT = SETTING['CHANGE_STATUS_TO_SUCCESS_LIMIT']
WALLET_NAME = SETTING['WALLET_NAME']
TESTING_MODE = SETTING['TESTING_MODE']
PSQL_USERNAME = SETTING['psqlUser']
PSQL_PASSWORD = SETTING['psqlPass']


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
				user=PSQL_USERNAME, \
				password=PSQL_PASSWORD, \
				host="localhost", \
				port="5432")
	message('Connection created')

	conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
	cur = conn.cursor()
	message('Cursor created')
	return conn, cur

def database_init(cur, conn):
	cur.execute('DROP SCHEMA IF EXISTS wpv1 cascade; DROP TYPE IF EXISTS \"status_setting\";')
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

def update_block_status(cur, height, status):
	cur.execute("IF EXISTS(SELECT * FROM mined_blocks WHERE height = %s) \
				UPDATE mined_blocks SET status = '%s' WHERE height = %s", \
				(height, status, height))

def get_block_status(cur):
	wallet_height = get_wallet_hight()

	cur.execute('SELECT status FROM mined_blocks WHERE height=%s', (wallet_height-10, ))
	working_block_status = cur.fetchone()

	if working_block_status is not None:
		working_block_status = working_block_status[0]
		if working_block_status in (0, 1):
			transfers = wallet_rpc('get_transfers', {'pool': True, 'out': True, \
													'in': True, 'pending': True, \
													'failed': True, 'filter_by_height': True, \
													'min_height': wallet_height-10, 'max_height': wallet_height-10})
			transfers = transfers['pool'] + transfers['out'] + transfers['in'] + \
						transfers['pending'] +transfers['failed']

			if transfers != []:
				update_block_status(cur, wallet_height-10, 2)
			else:
				update_block_status(cur, wallet_height-10, -1)

	cur.execute('SELECT status FROM mined_blocks WHERE height=%s', (wallet_height-60, ))
	working_block_status = cur.fetchone()

	if working_block_status is not None:
		working_block_status = working_block_status[0]
		if working_block_status == 2:
			transfers = wallet_rpc('get_transfers', {'pool': True, 'out': True, \
													'in': True, 'pending': True, \
													'failed': True, 'filter_by_height': True, \
													'min_height': wallet_height-60, 'max_height': wallet_height-60})
			transfers = transfers['pool'] + transfers['out'] + transfers['in'] + \
						transfers['pending'] +transfers['failed']

			if transfers != []:
				update_block_status(cur, wallet_height-60, 3)
			else:
				update_block_status(cur, wallet_height-60, -1)

	cur.execute('SELECT status FROM mined_blocks WHERE height=%s', (wallet_height, ))
	working_block_status = cur.fetchone()

	return_value = None
	if working_block_status is not None:
		return_value = working_block_status[0]

	return return_value

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
	if height != 0:
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

def submit_payment(cur, uid, amount, txid, txtime):
	cur.execute('INSERT INTO payments (uid, amount, txid, time, status) VALUES (%s, %s, %s, %s, %s)', \
						(uid, amount, txid, txtime, 'MONITORED'))

def get_balances_and_thresholds(cur):
	cur.execute("""SELECT
						uid,
						creSum,
						paySum,
						payTh.payment_threshold
					FROM (SELECT
						cuid AS uid,
						creRes.creSum,
						COALESCE(payRes.paySum, 0) AS paySum
					FROM (SELECT
						COALESCE(SUM(amount), 0) AS creSum,
						uid AS cuid
					FROM wpv1.credits
					GROUP BY cuid) AS creRes
					LEFT JOIN (SELECT
						COALESCE(SUM(amount), 0) AS paySum,
						uid AS puid
					FROM wpv1.payments
					GROUP BY puid) AS payRes
						ON puid = cuid) AS balance
					LEFT JOIN (SELECT
						uid AS tuid,
						payment_threshold
					FROM users) AS payTh
						ON tuid = uid""")

	results = cur.fetchall()
	return_value = []

	for result in results:
		return_value.append([result[0], result[1] - result[2], result[3]])

	return return_value

def get_new_payments(cur):
	balances_and_thresholds = get_balances_and_thresholds(cur)

	new_payments = []

	for item in balances_and_thresholds:
		if item[1] - item[2] >= 0:
			new_payments.append([item[0], int(item[1] / item[2]) * item[2]])

	return new_payments

def update_status(cur, txid, status):
	cur.execute("UPDATE payments SET status = '%s' WHERE txid = %s", (status, txid))

def check_payment_status(cur):
	current_block_height = get_wallet_hight()

	cur.execute('SELECT txid FROM payments WHERE status = \'MONITORED\'')
	txids = cur.fetchall()

	transfers = wallet_rpc('get_transfers', {'pool': True, 'out': True})
	transfers = transfers['pool'] + transfers['out']

	for txid in txids:
		txid = txid[0]
		is_in_list = False
		tx_height = 0
		for transfer in transfers:
			if transfer['txid'] == txid[0]:
				is_in_list = True
				tx_height = transfer['height']
				break

		if tx_height != 0:
			if current_block_height - tx_height >= CHANGE_STATUS_TO_SUCCESS_LIMIT:

				if is_in_list is True:
					update_status(cur, txid, 'SUCCESS')
				else:
					update_status(cur, txid, 'FAILED')

	message('Change status to success in height ' + str(current_block_height) + ' completed')

def calculate_credit(cur):
	wallet_height = get_wallet_hight()

	cur.execute('SELECT * FROM mined_blocks WHERE height=%s', (wallet_height-60, ))
	working_block = cur.fetchone()

	if working_block is not None:
		height = wallet_height-60
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

		for i, _ in enumerate(user_total_valid_share_in_block):
			user_total_valid_share_in_block[i]['credit'] = \
			user_total_valid_share_in_block[i]['valid_shares'] * \
			(BLOCK_REWARD * (1 - POOL_FEE) / total_valid_share_in_block)

			record_credit(cur, blk_id, user_total_valid_share_in_block[i]['uid'], \
								user_total_valid_share_in_block[i]['credit'])

		message('Block ' + str(height) + ' valid shares calculated')

		return height
	return None

def pay_payments(cur, new_payments):
	valid_payment_message = False

	for new_payment in new_payments:
		valid_payment_message = True

		destinations = []

		if new_payment != 0:

			user_wallet = get_user_wallet(cur, new_payment[0])

			destinations.append({'amount': new_payment[1], \
								'address': user_wallet})

			if destinations != []:
				json_data = {}
				transfer_info = {}

				txid = hexlify(urandom(32)).decode()

				if TESTING_MODE is True:
					json_data['tx_hash'] = 'TEST'
					transfer_info['transfer'] = {}
					transfer_info['transfer']['timestamp'] = 1536234479
				else:
					json_data = wallet_rpc('transfer', \
											{'destinations': destinations, 'payment_id': txid}) # 'get_tx_key': True
					transfer_info = wallet_rpc('get_transfer_by_txid', {'txid': json_data['tx_hash']})

				submit_payment(cur, new_payment[0], new_payment[1], json_data['tx_hash'], \
								transfer_info['transfer']['timestamp'])

				message('Pay ' + str(format(int(destinations[0]['amount'])/1000000000, '.9f')) + \
						' to ' + str(destinations[0]['address']))

	if valid_payment_message is True:
		message('Payments completed')

def get_wallet_hight():
	return wallet_rpc('getheight')['height']

try:
	CONN = None
	CURS = None
	CONN, CURS = connection_init()

	database_init(CURS, CONN)

	wallet_rpc('open_wallet', {'filename': WALLET_NAME, 'password': ''})

	while True:

		message('Block: ' + str(get_wallet_hight))

		check_payment_status(CURS)

		get_block_status(CURS)

		CALCULATED_BLOCK = calculate_credit(CURS)
		if CALCULATED_BLOCK is not None:
			update_block_status(CURS, CALCULATED_BLOCK, 4)

		NEW_PAYMENTS = get_new_payments(CURS)

		pay_payments(CURS, NEW_PAYMENTS)

		print()

		time.sleep(SLEEP_TIME)

except KeyboardInterrupt:
	print()
	message('Bye!!!')
	sys.exit()

except RuntimeError as my_exception:
	error(my_exception)
	traceback.print_exception(*sys.exc_info())
