import traceback
import sys
import datetime
import time
import json
from os import urandom
from binascii import hexlify
from pprint import pprint

import requests

import psycopg2

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from colorama import init as colorama_init, Fore

from settings import SETTING


colorama_init(autoreset=True)

# Read settings from settings file
SLEEP_TIME = SETTING['SLEEP_TIME']
BLOCK_REWARD = SETTING['BLOCK_REWARD']
POOL_FEE = SETTING['POOL_FEE'] # In %
SG_WALLET_RPC_ADDR = SETTING['SG_WALLET_RPC_ADDR_TESTNET']
TG_WALLET_RPC_AUTH = SETTING['TG_WALLET_RPC_AUTH_TESTNET']
SG_DAEMON_ADDR = SETTING['SG_DAEMON_ADDR_TESTNET']
CHANGE_STATUS_TO_SUCCESS_LIMIT = SETTING['CHANGE_STATUS_TO_SUCCESS_LIMIT']
WALLET_NAME = SETTING['WALLET_NAME']
TESTING_MODE = SETTING['TESTING_MODE']
PSQL_USERNAME = SETTING['psqlUser']
PSQL_PASSWORD = SETTING['psqlPass']
N = SETTING['N']
LAST_BLOCK = 0

def message(string):
	"""Print out messages"""
	string = str(string)
	print('[' + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + '] '\
	+ Fore.GREEN + 'Message: ' + Fore.RESET + string)
	with open('log.txt', 'a+') as f:
		f.write('[' + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + '] '\
	+ 'Message: ' + string + '\n')

def error(string):
	"""Print out error"""
	string = str(string)
	print('[' + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + '] '\
	+ Fore.RED + 'Error: ' + Fore.RESET + string)
	with open('log.txt', 'a+') as f:
		f.write('[' + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + '] '\
	+ 'Error: ' + string + '\n')

def connection_init():
	"""Create database connection"""
	conn = psycopg2.connect(user=PSQL_USERNAME,
							password=PSQL_PASSWORD,
							host="localhost",
							port="5432")
	message('Connection created')

	conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
	cur = conn.cursor()
	message('Cursor created')
	return conn, cur

def connection_destroy(conn, cur):
	"""Destroy database connection"""
	cur.close()
	conn.close()

def database_init(cur, conn):
	"""Drop existing schema and types and recreate them"""
	# Drop existing schema and type
	cur.execute('DROP SCHEMA IF EXISTS wpv1 cascade; DROP TYPE IF EXISTS \"status_setting\";')
	conn.commit()
	message('Schema wpv1 droped')

	# Recreate schema and types
	with open('CREATE_DB.sql', 'r') as my_file:
		cur.execute(my_file.read())
	conn.commit()
	message('Schema wpv1 created')

	# Insert example data
	with open('DATA.sql', 'r') as my_file:
		cur.execute(my_file.read())
	conn.commit()
	message('Data added')

	# Set search path that I don't every time specify schema dot table
	cur.execute('SET search_path TO wpv1')
	conn.commit()
	message('Set search_path to wpv1')

def change_block_status(cur, height, status):
	"""Change a block status to given status"""
	cur.execute("""DO $$
					BEGIN
						IF EXISTS(SELECT * FROM mined_blocks WHERE height = %s)
						THEN UPDATE mined_blocks SET status = '%s' WHERE height = %s;
						END IF;
					END
					$$ ;""",
				(height, status, height))

def update_block_status(cur):
	"""Update blocks statuses"""
	wallet_height = get_wallet_hight()

	# Get submit failed(status-0) and submit OK(status-1) blocks that are in a safe distance(10)
	cur.execute('SELECT height FROM mined_blocks WHERE (status=1 OR status=0) AND height <= %s',
				(wallet_height-10,))
	heights = cur.fetchall()

	# For each status-0 or status-1 block
	for height in heights:
		height = height[0]
		# Get its transactions
		transfers = wallet_rpc('get_transfers', {'pool': True, 'out': True,
												'in': True, 'pending': True,
												'failed': True, 'filter_by_height': True,
												'min_height': height, 'max_height': height})
		temp = []
		if 'pool' in transfers:
			temp += transfers['pool']
		if 'out' in transfers:
			temp += transfers['out']
		if 'in' in transfers:
			temp += transfers['in']
		if 'pending' in transfers:
			temp += transfers['pending']
		if 'failed' in transfers:
			temp += transfers['failed']
		transfers = temp

		# If there is no transaction update block status to -1(Confirmed orphan) else to 2(Transaction seen)
		if transfers != []:
			change_block_status(cur, height, 2)
		else:
			change_block_status(cur, height, -1)

	# Get status-3 blocks that are in a safe distance(60 blocks)
	cur.execute('SELECT height FROM mined_blocks WHERE status=3 AND height <= %s',
				(wallet_height-60,)) # Q: It should be wallet_height or wallet_height-60?
	heights = cur.fetchall()

	status_3_blocks = []

	for height in heights:
		status_3_blocks.append(height[0])

	# Get status-2 blocks that are in safe distance
	cur.execute('SELECT height FROM mined_blocks WHERE status=2 AND height <= %s',
				(wallet_height-60,))
	heights = cur.fetchall()

	# For each status-2 block
	for height in heights:
		height = height[0]
		# Q: Is it necessary to get block-2 tansactions?
		transfers = wallet_rpc('get_transfers', {'pool': True, 'out': True,
												'in': True, 'pending': True,
												'failed': True, 'filter_by_height': True,
												'min_height': height, 'max_height': height})
		temp = []
		if 'pool' in transfers:
			temp += transfers['pool']
		if 'out' in transfers:
			temp += transfers['out']
		if 'in' in transfers:
			temp += transfers['in']
		if 'pending' in transfers:
			temp += transfers['pending']
		if 'failed' in transfers:
			temp += transfers['failed']
		transfers = temp

		# Change block status-2 blocks status's to 3
		if transfers != []:
			change_block_status(cur, height, 3)
			status_3_blocks.append(height)
		else:
			change_block_status(cur, height, -1)

	return status_3_blocks

def wallet_rpc(s_method, d_params=None):
	"""Call wallet RPC"""
	d_headers = {'Content-Type': 'application/json'}
	d_rpc_input = {"jsonrpc": "2.0", "id": "0", "method" :  s_method}

	if d_params is not None:
		d_rpc_input['params'] = d_params

	o_rsp = requests.post('http://' + SG_WALLET_RPC_ADDR + '/json_rpc',
							data=json.dumps(d_rpc_input),
							headers=d_headers,
							timeout=60.0, #Wallet can be fairly slow for large requests
							auth=requests.auth.HTTPDigestAuth(*TG_WALLET_RPC_AUTH))

	if o_rsp.status_code != requests.codes.ok: # pylint: disable=maybe-no-member
		raise RuntimeError('HTTP Request error : ' + o_rsp.reason)

	d_jsn = o_rsp.json()
	if 'error' in d_jsn:
		raise RuntimeError("Wallet error: " + d_jsn['error']['message'])

	return d_jsn['result']

def daemon_rpc(s_method, d_params=None):
	d_headers = {'Content-Type': 'application/json'}
	d_daemon_input = {"jsonrpc": "2.0", "id": "0", "method" :  s_method}

	if d_params is not None:
		d_daemon_input['params'] = d_params

	o_rsp = requests.post('http://' + SG_DAEMON_ADDR + '/json_rpc',
							data=json.dumps(d_daemon_input),
							headers=d_headers,
							timeout=60.0)  #Wallet can be fairly slow for large requests

	if o_rsp.status_code != requests.codes.ok: # pylint: disable=maybe-no-member
		raise RuntimeError('HTTP Request error : ' + o_rsp.reason)

	d_jsn = o_rsp.json()
	if 'error' in d_jsn:
		raise RuntimeError("Wallet error: " + d_jsn['error']['message'])

	return d_jsn['result']

def get_user_wallet(cur, uid):
	"""Get users wallet address"""
	cur.execute('SELECT wallet FROM users WHERE uid=%s', (uid, ))
	return cur.fetchone()[0]

def record_credit(cur, blk_id, uid, credit):
	"""Record calculated credit for a user"""
	cur.execute('INSERT INTO credits (blk_id, uid, amount) VALUES (%s, %s, %s)',
					(blk_id, uid, credit))

def submit_payment(cur, uid, amount, txid, txtime):
	"""Submit payed payment"""
	cur.execute('INSERT INTO payments (uid, amount, txid, time, status) VALUES (%s, %s, %s, %s, %s)',
						(uid, amount, txid, txtime, 'MONITORED'))

def get_balances_and_thresholds(cur):
	"""Get users sum of credits, sum of payment and threshold"""
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
		# Calculate diffrence of sum of credits and sum of payments for each user
		return_value.append([result[0], result[1] - result[2], result[3]])

	return return_value

def update_status(cur, txid, status):
	"""Change payment status to SUCCESS or FAILED or MONITORED"""
	cur.execute("UPDATE payments SET status = '%s' WHERE txid = %s", (status, txid))

def update_payment_status(cur):
	"""Update MONITORED payments to SUCCESS or FAILED"""
	current_block_height = get_wallet_hight()

	# Get MONITORED payments
	cur.execute('SELECT txid FROM payments WHERE status = \'MONITORED\'')
	txids = cur.fetchall()

	# Get outgoing and daemon's transaction pool transfers
	transfers = wallet_rpc('get_transfers', {'pool': True, 'out': True})
	temp = []
	if 'out' in transfers:
		temp += transfers['out']
	if 'pool' in transfers:
		temp += transfers['pool']
	transfers = temp

	# For each transaction
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
			# If it is in a safe distance
			if current_block_height - tx_height >= CHANGE_STATUS_TO_SUCCESS_LIMIT:

				# If transaction is in list it successed else it failed
				if is_in_list is True:
					update_status(cur, txid, 'SUCCESS')
				else:
					update_status(cur, txid, 'FAILED')

	message('Change status to success in height ' + str(current_block_height) + ' completed')

def make_N_shares(cur):
	"""Find last N shares based on PPLNS system"""
	cur.execute("""SELECT height, time 
					FROM wpv1.mined_blocks 
					WHERE status=3 
					ORDER BY time""")
	times = cur.fetchall()
	temp = []
	for t in times:
		block_temp = [t[0], t[1]]
		block_temp.append(daemon_rpc('get_block', {'height': t[0]})['block_header']['difficulty']*2)
		temp.append(block_temp)
	times = temp
	retval = {}
	for t in times:
		flag = False
		limiter = 1
		last_result = 0
		result_flag = False
		limiter_checker = 10
		while flag is False:
			cur.execute("""SELECT COALESCE(SUM(temp2.sum), 0) AS sum
							FROM
								(SELECT uid, SUM(count) 
								FROM
									(SELECT uid, count 
									FROM wpv1.valid_shares 
									WHERE time <= %s
									ORDER BY TIME DESC LIMIT %s) AS temp 
								GROUP BY uid) AS temp2""", (t[1], limiter))
			result = cur.fetchone()

			if result[0] == last_result:
				limiter_checker -= 1
				if limiter_checker == 0:
					flag = True
					result_flag = True
			else:
				limiter_checker = 10

			last_result = result[0]

			if result[0] >= t[2]:
				flag = True
			elif result[0] == 0:
				limiter = 0
				break
			else:
				limiter += 1

		cur.execute("""SELECT uid, count
						FROM wpv1.valid_shares 
						WHERE time <= %s
						ORDER BY TIME DESC LIMIT %s""", (t[1], limiter))
	
		result = cur.fetchall()

		counter = 0

		retval[t[0]] = {}
		if result_flag is False:
			for i in result:
				if str(i[0]) not in retval[t[0]]:
					retval[t[0]][str(i[0])] = 0
				if counter + i[1] > t[2]:
					retval[t[0]][str(i[0])] += t[2] - counter # i[1] - ((counter + i[1]) - N)
				else:
					retval[t[0]][str(i[0])] += i[1]
					counter += i[1]

			for k in retval[t[0]]:
				retval[t[0]][k] = int((BLOCK_REWARD*(retval[t[0]][k]/t[2]))*(1-(POOL_FEE/100)))
				message('Block ' + str(t[0]) + ' credits ' + str(format(int(retval[t[0]][k])/1000000000, '.9f'))\
						+ ' for user ' + k + '.')
		else:
			for i in result:
				if str(i[0]) not in retval[t[0]]:
					retval[t[0]][str(i[0])] = 0
				if counter + i[1] > last_result:
					retval[t[0]][str(i[0])] += last_result - counter # i[1] - ((counter + i[1]) - N)
				else:
					retval[t[0]][str(i[0])] += i[1]
					counter += i[1]

			for k in retval[t[0]]:
				retval[t[0]][k] = int((BLOCK_REWARD*(retval[t[0]][k]/float(last_result)))*(1-(POOL_FEE/100)))
				message('Block ' + str(t[0]) + ' credits ' + str(format(int(retval[t[0]][k])/1000000000, '.9f'))\
						+ ' for user ' + k + '.')

		message('Credits for block ' + str(t[0]) + ' calculated.')

	return retval

def get_max_block_id(cur):
	"""Get latest mined block block ID"""
	cur.execute("""SELECT MAX(blk_id) FROM mined_blocks""")
	return cur.fetchone()[0]

def get_block_id(cur, height):
	"""Get block id of height"""
	cur.execute("""SELECT blk_id FROM mined_blocks WHERE height=%s""", (height,))
	return cur.fetchone()[0]

def calculate_credit(cur):
	"""Calculate credits using PPLNS system"""

	# Get payment for each miner
	N_shares = make_N_shares(cur)

	# Record calculated credit with PPLNS system for each miner
	for height in N_shares:
		for uid in N_shares[height]:
			blk_id = get_block_id(cur, height)
			record_credit(cur, blk_id, uid, N_shares[height][uid])
		change_block_status(cur, height, 4)

def pay_payments(cur):
	"""Pay payments base on credits and paid payments"""
	destinations = []
	messages = {}
	new_payments = {}

	user_payment_info = get_balances_and_thresholds(cur)

	for i in user_payment_info:
		if i[1] >= i[2]:
			new_payments[i[0]] = int(i[1])

	for uid in new_payments:
		user_wallet = get_user_wallet(cur, uid)

		amount = new_payments[uid]
		destinations.append({'amount': amount,
								'address': user_wallet})
		messages[uid] = {'amount': amount, 'address': user_wallet}

	if destinations != []:
		json_data = {}
		transfer_info = {}

		txid = hexlify(urandom(32)).decode()

		if TESTING_MODE is True:
			json_data['tx_hash'] = 'TEST'
			transfer_info['transfer'] = {}
			transfer_info['transfer']['timestamp'] = 1536234479
		else:
			json_data = wallet_rpc('transfer',
									{'destinations': destinations, 'payment_id': txid}) # 'get_tx_key': True
			transfer_info = wallet_rpc('get_transfer_by_txid', {'txid': json_data['tx_hash']})



		for uid in messages:
			submit_payment(cur, uid, messages[uid]['amount'], json_data['tx_hash'],
						transfer_info['transfer']['timestamp'])

			message('Pay ' + str(format(int(messages[uid]['amount'])/1000000000, '.9f')) + \
					' to ' + str(messages[uid]['address']) + ' for user ' + str(uid) + '.')

		message('Payments completed')

def get_wallet_hight():
	"""Get the wallet's current block height"""
	return wallet_rpc('getheight')['height']

def wait_until_new_block(cur):
	"""Wait until new block mined"""
	global LAST_BLOCK
	result = LAST_BLOCK
	while result == LAST_BLOCK:
		cur.execute("""SELECT MAX(blk_id) FROM mined_blocks""")
		result = cur.fetchone()[0]

		time.sleep(SLEEP_TIME/2)

	LAST_BLOCK = result

try:
	CONN = None
	CURS = None
	CONN, CURS = connection_init()

	database_init(CURS, CONN)

	wallet_rpc('open_wallet', {'filename': WALLET_NAME, 'password': ''})

	while True:

		message('Block: ' + str(get_wallet_hight()))

		update_payment_status(CURS)

		update_block_status(CURS)

		calculate_credit(CURS)

		pay_payments(CURS)

		print()

		wait_until_new_block(CURS)

except KeyboardInterrupt:
	print()
	message('Bye!!!')
	sys.exit()

except RuntimeError as my_exception:
	error(my_exception)
	traceback.print_exception(*sys.exc_info())

else:
	connection_destroy(CONN, CURS)
