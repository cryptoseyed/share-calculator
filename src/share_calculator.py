import traceback
import sys
import datetime
import time
import json
from os import urandom
from binascii import hexlify

import requests

import psycopg2

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from colorama import init as colorama_init, Fore

from settings import SETTING


colorama_init(autoreset=True)


SLEEP_TIME = SETTING['SLEEP_TIME']
BLOCK_REWARD = SETTING['BLOCK_REWARD']
POOL_FEE = SETTING['POOL_FEE'] # In %
SG_WALLET_RPC_ADDR = SETTING['SG_WALLET_RPC_ADDR_TESTNET']
TG_WALLET_RPC_AUTH = SETTING['TG_WALLET_RPC_AUTH_TESTNET']
CHANGE_STATUS_TO_SUCCESS_LIMIT = SETTING['CHANGE_STATUS_TO_SUCCESS_LIMIT']
WALLET_NAME = SETTING['WALLET_NAME']
TESTING_MODE = SETTING['TESTING_MODE']
PSQL_USERNAME = SETTING['psqlUser']
PSQL_PASSWORD = SETTING['psqlPass']
N = SETTING['N']
LAST_BLOCK = 0

def message(string):
	string = str(string)
	print('[' + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + '] '\
	+ Fore.GREEN + 'Message: ' + Fore.RESET + string)

def error(string):
	string = str(string)
	print('[' + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + '] '\
	+ Fore.RED + 'Error: ' + Fore.RESET + string)

def connection_init():
	conn = psycopg2.connect(user=PSQL_USERNAME,
							password=PSQL_PASSWORD,
							host="localhost",
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
	cur.execute("""IF EXISTS(SELECT * FROM mined_blocks WHERE height = %s)
				UPDATE mined_blocks SET status = '%s' WHERE height = %s""",
				(height, status, height))

def get_block_status(cur):
	wallet_height = get_wallet_hight()

	cur.execute('SELECT height FROM mined_blocks WHERE (status=1 OR status=0) AND height <= %s',
				(wallet_height-10,))
	heights = cur.fetchall()

	for height in heights:
		height = height[0]
		transfers = wallet_rpc('get_transfers', {'pool': True, 'out': True,
												'in': True, 'pending': True,
												'failed': True, 'filter_by_height': True,
												'min_height': height, 'max_height': height})
		transfers = transfers['pool'] + transfers['out'] + transfers['in'] + \
					transfers['pending'] +transfers['failed']

		if transfers != []:
			update_block_status(cur, height, 2)
		else:
			update_block_status(cur, height, -1)

	cur.execute('SELECT height FROM mined_blocks WHERE status=3 AND height <= %s',
				(wallet_height-60,))
	heights = cur.fetchall()

	changed_status_to_3_blocks = []

	for height in heights:
		changed_status_to_3_blocks.append(height[0])

	cur.execute('SELECT height FROM mined_blocks WHERE status=2 AND height <= %s',
				(wallet_height-60,))
	heights = cur.fetchall()

	for height in heights:
		height = height[0]
		transfers = wallet_rpc('get_transfers', {'pool': True, 'out': True,
												'in': True, 'pending': True,
												'failed': True, 'filter_by_height': True,
												'min_height': height, 'max_height': height})
		transfers = transfers['pool'] + transfers['out'] + transfers['in'] + \
					transfers['pending'] +transfers['failed']

		if transfers != []:
			update_block_status(cur, height, 3)
			changed_status_to_3_blocks.append(height)
		else:
			update_block_status(cur, height, -1)

	return changed_status_to_3_blocks

def wallet_rpc(s_method, d_params=None):
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

def get_user_wallet(cur, uid):
	cur.execute('SELECT wallet FROM users WHERE uid=%s', (uid, ))
	return cur.fetchone()[0]

def record_credit(cur, blk_id, uid, credit):
	cur.execute('INSERT INTO credits (blk_id, uid, amount) VALUES (%s, %s, %s)',
					(blk_id, uid, credit))

def submit_payment(cur, uid, amount, txid, txtime):
	cur.execute('INSERT INTO payments (uid, amount, txid, time, status) VALUES (%s, %s, %s, %s, %s)',
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

def make_N_shares(cur):
	flag = False
	limiter = 1
	while flag is False:
		cur.execute("""SELECT COALESCE(SUM(temp2.sum), 0) AS sum
						FROM
							(SELECT uid, SUM(count) 
							FROM
								(SELECT uid, count 
								FROM wpv1.valid_shares 
								ORDER BY TIME DESC LIMIT %s) AS temp 
							GROUP BY uid) AS temp2""", (limiter,))
		result = cur.fetchone()
		if result[0] >= N:
			flag = True
		else:
			limiter += 1
	
	cur.execute("""SELECT uid, SUM(count) 
					FROM
						(SELECT uid, count 
						FROM wpv1.valid_shares 
						ORDER BY TIME DESC LIMIT %s) AS temp 
					GROUP BY uid""")
	
	result = cur.fetchall()

	counter = 0

	retval = {}

	for i in result:
		if counter + i[1] > N:
			retval[str(i[0])] = (counter + i[1]) - N
		else:
			retval[i[0]] = i[1]
			counter += i[1]
	
	return retval

def get_max_block_id(cur):
	cur.execute("""SELECT MAX(blk_id) FROM mined_blocks""")
	return cur.fetchone()[0]

def calculate_credit(cur):
	# Credits must calculate here and then pay_payments should do its work there
	blk_id = get_max_block_id(cur)

	N_shares = make_N_shares(cur)

	for uid in N_shares:
		record_credit(cur, blk_id, uid, N_shares[uid])

def pay_payments(cur):
	destinations = []
	messages = {}
	new_payments = {}

	user_payment_info = get_balances_and_thresholds(cur)

	for i in user_payment_info:
		if i[1] >= i[2]:
			new_payments[i[0]] = i[1]

	for uid in new_payments:
		user_wallet = get_user_wallet(cur, uid)

		amount = (BLOCK_REWARD*(new_payments[uid]/N))*(1-(POOL_FEE/100))
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
					' to ' + str(messages[uid]['address']))

		message('Payments completed')

def get_wallet_hight():
	return wallet_rpc('getheight')['height']

def wait_until_new_block(cur):
	result = LAST_BLOCK
	while result == LAST_BLOCK:
		cur.execute("""SELECT MAX(blk_id) FROM mined_blocks""")
		result = cur.fetchone[0]

		time.sleep(SLEEP_TIME/2)

	result = LAST_BLOCK

try:
	CONN = None
	CURS = None
	CONN, CURS = connection_init()

	database_init(CURS, CONN)

	wallet_rpc('open_wallet', {'filename': WALLET_NAME, 'password': ''})

	while True:

		message('Block: ' + str(get_wallet_hight()))

		check_payment_status(CURS)

		get_block_status(CURS)

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
