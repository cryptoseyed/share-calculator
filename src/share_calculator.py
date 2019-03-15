import traceback
import sys
import datetime
import time
import json

import requests

import psycopg2

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from colorama import init as colorama_init, Fore
from pprint import pprint
from math import floor

from settings import SETTING


colorama_init(autoreset=True)

# Read settings from settings file
SLEEP_TIME = SETTING['SLEEP_TIME']
POOL_FEE = SETTING['POOL_FEE'] # In %
SG_WALLET_RPC_ADDR = SETTING['SG_WALLET_RPC_ADDR_TESTNET']
TG_WALLET_RPC_AUTH = SETTING['TG_WALLET_RPC_AUTH_TESTNET']
SG_DAEMON_ADDR = SETTING['SG_DAEMON_ADDR_TESTNET']
CHANGE_STATUS_TO_SUCCESS_LIMIT = SETTING['CHANGE_STATUS_TO_SUCCESS_LIMIT']
WALLET_NAME = SETTING['WALLET_NAME']
TESTING_MODE = SETTING['TESTING_MODE']
PSQL_USERNAME = SETTING['psqlUser']
PSQL_PASSWORD = SETTING['psqlPass']
LAST_BLOCK = 0
FEE_PER_KB = SETTING['FEE_PER_KB']
FEE_PER_RING_MEMBER = SETTING['FEE_PER_RING_MEMBER']
TRANSFER_RING_SIZE = SETTING['TRANSFER_RING_SIZE']
TRANSFER_PRIORITY = SETTING['TRANSFER_PRIORITY']
TRANSFER_MAX_RECIPIENTS = SETTING['TRANSFER_MAX_RECIPIENTS']
PAYMENT_INTERVAL = SETTING['PAYMENT_INTERVAL']

def message(string):
	"""Print out messages"""
	string = str(string)
	print(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') \
	+ Fore.GREEN + ' Message - ' + Fore.RESET + string)
	with open('share_claculator.log', 'a+') as f:
		f.write(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') \
	+ ' Message - ' + string + '\n')

def message_wallet_rpc(REQorRES, data):
	"""Print out messages for wallet RPC calls"""
	if REQorRES == 'req':
		print(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') \
		+ Fore.GREEN + ' [Request] Wallet RPC' + Fore.RESET)
	elif REQorRES == 'res':
		print(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') \
		+ Fore.GREEN + ' [Response] Wallet RPC' + Fore.RESET)
	else:
		return
	# pprint(data)
	with open('Wallet_RPC_Calls.log', 'a+') as f:
		if REQorRES == 'req':
			f.write(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') \
			+ ' [Request] Wallet RPC' + '\n')
		elif REQorRES == 'res':
			f.write(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') \
			+ ' [Response] Wallet RPC' + '\n')
		pprint(data, f)
		f.write('\n')

def message_daemon_rpc(REQorRES, data):
	"""Print out messages for daemon RPC calls"""
	if REQorRES == 'req':
		print(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') \
		+ Fore.GREEN + ' [Request] Daemon RPC' + Fore.RESET)
	elif REQorRES == 'res':
		print(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') \
		+ Fore.GREEN + ' [Response] Daemon RPC' + Fore.RESET)
	else:
		return
	# pprint(data)
	with open('Daemon_RPC_Calls.log', 'a+') as f:
		if REQorRES == 'req':
			f.write(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') \
			+ ' [Request] Daemon RPC' + '\n')
		elif REQorRES == 'res':
			f.write(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') \
			+ ' [Response] Daemon RPC' + '\n')
		pprint(data, f)
		f.write('\n')

def error(string):
	"""Print out error"""
	string = str(string)
	print(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') \
	+ Fore.RED + ' Error - ' + Fore.RESET + string)
	with open('share_claculator.log', 'a+') as f:
		f.write(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') \
	+ ' Error - ' + string + '\n')

def connection_init():
	"""Create database connection"""
	try:
		conn = psycopg2.connect(user=PSQL_USERNAME,
								password=PSQL_PASSWORD,
								host="localhost",
								port="5432")
		message('Database connection created')
	except:
		error('Failed to creat database connection')
		sys.exit()

	conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
	try:
		cur = conn.cursor()
		message('Database cursor created')
	except:
		error('Failed to create database cursor')
	return conn, cur

def close_connection(conn, cur):
	"""Destroy database connection"""
	cur.close()
	message('Database cursor closed')
	conn.close()
	message('Database connection closed')

def database_init(cur, conn):
	"""Drop existing schema and types and recreate them"""
	try:
		# Drop existing schema and type
		cur.execute('DROP SCHEMA IF EXISTS wpv1 cascade; DROP TYPE IF EXISTS \"status_setting\";')
		conn.commit()
		message('Database schema wpv1 droped')

		# Recreate schema and types
		with open('CREATE_DB.sql', 'r') as my_file:
			cur.execute(my_file.read())
		conn.commit()
		message('Database schema wpv1 created')

		# Insert example data
		with open('DATA.sql', 'r') as my_file:
			cur.execute(my_file.read())
		conn.commit()
		message('Database test data added')

		# Set search path that I don't every time specify schema dot table
		cur.execute('SET search_path TO wpv1')
		conn.commit()
		message('Database search_path set to wpv1')
	except:
		error('Failed to initialize database data')

def change_block_status(cur, height, status):
	"""Change a block status to given status"""
	try:
		cur.execute("""DO $$
						BEGIN
							IF EXISTS(SELECT * FROM mined_blocks WHERE height = %s)
							THEN UPDATE mined_blocks SET status = '%s' WHERE height = %s;
							END IF;
						END
						$$ ;""",
					(height, status, height))
		message('Change block ' + str(height) + ' status to ' + str(status))
	except:
		error('Failed to change block ' + str(height) + ' status to ' + str(status))

def update_block_status(cur):
	"""Update blocks statuses"""
	
	wallet_height = get_wallet_height()

	# Get submit failed(status-0) and submit OK(status-1) blocks that are in a safe distance(10)
	cur.execute('SELECT height, txid FROM mined_blocks WHERE (status=1 OR status=0) AND height <= %s',
				(wallet_height-10,))
	blocks = cur.fetchall()

	# For each status-0 or status-1 block
	for block in blocks:
		height = block[0]
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

		# If the block txid seen update block status to -1(Confirmed orphan) else to 2(Transaction seen)
		block_txid = block[1]
		flag = False

		for t in transfers:
			if t['txid'] == block_txid:
				flag = True
				break

		if flag == True:
			change_block_status(cur, height, 2)
		else:
			change_block_status(cur, height, -1)

	# Get status-2 blocks that are in safe distance
	cur.execute('SELECT height, txid FROM mined_blocks WHERE status=2 AND height <= %s',
				(wallet_height-60,))
	blocks = cur.fetchall()

	# For each status-2 block
	for block in blocks:
		height = block[0]
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

		# Change block status-2 blocks status's to 3 if the block txid seen
		block_txid = block[1]
		flag = False

		for t in transfers:
			if t['txid'] == block_txid:
				flag = True
				break

		if flag == True:
			change_block_status(cur, height, 3)
		else:
			change_block_status(cur, height, -1)

	# Get status-3 blocks
	cur.execute('SELECT height FROM mined_blocks WHERE status=3')
	heights = cur.fetchall()

	status_3_blocks = []

	for height in heights:
		status_3_blocks.append(height[0])

	return status_3_blocks

def wallet_rpc(s_method, d_params=None):
	"""Call wallet RPC"""
	try:
		d_headers = {'Content-Type': 'application/json'}
		d_rpc_input = {"jsonrpc": "2.0", "id": "0", "method" :  s_method}

		if d_params is not None:
			d_rpc_input['params'] = d_params

		message_wallet_rpc('req', d_rpc_input)

		o_rsp = requests.post('http://' + SG_WALLET_RPC_ADDR + '/json_rpc',
								data=json.dumps(d_rpc_input),
								headers=d_headers,
								timeout=60.0, #Wallet can be fairly slow for large requests
								auth=requests.auth.HTTPDigestAuth(*TG_WALLET_RPC_AUTH))

		if o_rsp.status_code != requests.codes.ok: # pylint: disable=maybe-no-member
			raise RuntimeError('Wallet RPC HTTP Request error : ' + o_rsp.reason)

		d_jsn = o_rsp.json()

		message_wallet_rpc('res', d_jsn)

		if 'error' in d_jsn:
			raise RuntimeError('Wallet RPC wallet error: ' + d_jsn['error']['message'])

		return d_jsn['result']
	except RuntimeError as re:
		error(re)
		raise RuntimeError(re)
	except KeyboardInterrupt:
		print()
		message('Bye!!!')
		sys.exit()
	except:
		error('Wallet RPC error')

def daemon_rpc(s_method, d_params=None):
	try:
		d_headers = {'Content-Type': 'application/json'}
		d_daemon_input = {"jsonrpc": "2.0", "id": "0", "method" :  s_method}

		if d_params is not None:
			d_daemon_input['params'] = d_params

		message_daemon_rpc('req', d_daemon_input)

		o_rsp = requests.post('http://' + SG_DAEMON_ADDR + '/json_rpc',
								data=json.dumps(d_daemon_input),
								headers=d_headers,
								timeout=60.0)  #Wallet can be fairly slow for large requests

		if o_rsp.status_code != requests.codes.ok: # pylint: disable=maybe-no-member
			raise RuntimeError('HTTP Request error : ' + o_rsp.reason)

		d_jsn = o_rsp.json()

		message_daemon_rpc('res', d_jsn)

		if 'error' in d_jsn:
			raise RuntimeError("Wallet error: " + d_jsn['error']['message'])

		return d_jsn['result']
	except RuntimeError as re:
		error(re)
	except KeyboardInterrupt:
		print()
		message('Bye!!!')
		sys.exit()
	except:
		error('Daemon RPC error')

def get_user_wallet(cur, uid):
	"""Get users wallet address"""
	try:
		cur.execute('SELECT wallet FROM users WHERE uid=%s', (uid, ))
		return cur.fetchone()[0]
	except:
		error('Failed to get user wallet address for user ' + str(uid))

def record_credit(cur, blk_id, uid, credit):
	"""Record calculated credit for a user"""
	try:
		cur.execute('INSERT INTO credits (blk_id, uid, amount) VALUES (%s, %s, %s)',
						(blk_id, uid, credit))
		message('Record ' + str(credit) + ' credit(s) for user ' + str(uid))
	except:
		error('Failed to record ' + str(credit) + ' credit(s) for user ' + str(uid))

def submit_payment(cur, uid, amount, txid, txtime, fee):
	"""Submit payed payment"""
	try:
		cur.execute('INSERT INTO payments (uid, amount, txid, time, status) VALUES (%s, %s, %s, %s, %s), (%s, %s, %s, %s, %s)',
							(uid, amount, txid, txtime, 'MONITORED', uid, fee, txid, txtime, 'FEE'))
		message('Record ' + str(amount) + ' payment(s) for user ' + str(uid))
		return True
	except:
		error('FAiled to record ' + str(amount) + ' payment(s) for user ' + str(uid))
		return False

def get_balances_and_thresholds(cur):
	"""Get users sum of credits, sum of payment and threshold"""
	try:
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
										WHERE status != 'FAILED'
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
	except Exception as e:
		print(e)
		error('Failed to get balances and thresholds')

def update_status(cur, txid, status):
	"""Change payment status to SUCCESS or FAILED or MONITORED"""
	try:
		cur.execute("UPDATE payments SET status = '%s' WHERE txid = %s", (status, txid))
		message('Update payment status to ' + str(status) + ' for txid ' + str(txid))
	except:
		error('Failed to update payment status to ' + str(status) + ' for txid ' + str(txid))

def update_payment_status(cur):
	"""Update MONITORED payments to SUCCESS or FAILED"""
	current_block_height = get_wallet_height()

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
		daemon_responce = daemon_rpc('get_block', {'height': t[0]})
		block_temp.append(daemon_responce['block_header']['difficulty']*2)
		block_temp.append(daemon_responce['block_header']['reward'])
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
				retval[t[0]][k] = int(((t[3])*(retval[t[0]][k]/t[2]))*(1-(POOL_FEE/100)))
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
				retval[t[0]][k] = int(((t[3])*(retval[t[0]][k]/float(last_result)))*(1-(POOL_FEE/100)))
				message('Block ' + str(t[0]) + ' credits ' + str(format(int(retval[t[0]][k])/1000000000, '.9f'))\
						+ ' for user ' + k + '.')

		message('Credits for block ' + str(t[0]) + ' calculated.')

	return retval

def get_block_id(cur, height):
	"""Get block id of height"""
	try:
		cur.execute("""SELECT blk_id FROM mined_blocks WHERE height=%s""", (height,))
		return cur.fetchone()[0]
	except:
		error('Failed to get blk_id for block ' + str(height))

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

def calculate_fee(ring_size, bytes_no, fee_multiplier):
	fee = floor((bytes_no + 1023) / 1024) * FEE_PER_KB
	fee += ring_size * FEE_PER_RING_MEMBER
	return fee * fee_multiplier

def estimate_rct_tx_size(n_inputs, mixin, n_outputs, extra_size, bulletproof):
	size = 0

	#  tx prefix

	# first few bytes
	size += 1 + 6

	# vin
	size += n_inputs * (1 + 6 + (mixin + 1) * 2 + 32)

	# vout
	size += n_outputs * (6 + 32)

# extra
	size += extra_size

	# rct signatures

	# type
	size += 1

	# rangeSigs
	if bulletproof:
		size += ((2 * 6 + 4 + 5) * 32 + 3) * n_outputs
	else:
		size += (2 * 64 * 32 + 32 + 64 * 32) * n_outputs

# MGs
	size += n_inputs * (64 * (mixin + 1) + 32)

	# mixRing - not serialized, can be reconstructed
	# size += 2 * 32 * (mixin+1) * n_inputs

	# pseudoOuts
	size += 32 * n_inputs
	# ecdhInfo
	size += 2 * 32 * n_outputs
	# outPk - only commitment is saved
	size += 32 * n_outputs
	# txnFee
	size += 4

	return size

def pay_payments(cur):
	"""Pay payments base on credits and paid payments"""
	destinations = []
	destinations_uid = []
	destinations_counter = 1
	payment_id_destinations = []
	payment_id_destinations_uid = []
	new_payments = {}

	user_payment_info = get_balances_and_thresholds(cur)

	for i in user_payment_info:
		if i[1] >= i[2]:
			new_payments[i[0]] = int(i[1])

	for uid in new_payments:
		user_wallet = get_user_wallet(cur, uid)

		amount = 100000000 # new_payments[uid]
		if len(destinations) != destinations_counter:
			destinations.append([])
			destinations_uid.append([])
		if '.' in user_wallet:
			payment_id_destinations.append([{'amount': amount,
											'address': user_wallet.split('.')[0]}])
			payment_id_destinations_uid.append([{'uid': uid, 'payment_id': user_wallet.split('.')[1], 'fee': 0}])
		elif user_wallet.startswith('RYoE'):
			payment_id_destinations.append([{'amount': amount,
											'address': user_wallet}])
			payment_id_destinations_uid.append([{'uid': uid, 'payment_id': '', 'fee': 0}])

		else:
			destinations[destinations_counter-1].append({'amount': amount,
														'address': user_wallet})
			destinations_uid[destinations_counter-1].append({'uid': uid, 'payment_id': '', 'fee': 0})

		if len(destinations[destinations_counter-1]) == TRANSFER_MAX_RECIPIENTS:
			destinations_counter += 1

	if TESTING_MODE is True:
		with open('Payments', 'a+') as f:
			f.writelines('destinations:\n')
			pprint(destinations, f)
			f.writelines('destinations_uid:\n')
			pprint(destinations_uid, f)
			f.writelines('payment_id_destinations:\n')
			pprint(payment_id_destinations, f)
			f.writelines('payment_id_destinations_uid:\n')
			pprint(payment_id_destinations_uid, f)
			f.writelines('\n\n\n\n\n')

	process_payment(cur, destinations, destinations_uid)

	process_payment(cur, payment_id_destinations, payment_id_destinations_uid)

	message('Payments completed')

def process_payment(cur, dest, uids):
	"""Process payment"""
	for i in range(len(dest)):
		json_data = {}
		transfer_info = {}

		if TESTING_MODE is True:
			bytes_no = estimate_rct_tx_size(n_inputs=2, mixin=TRANSFER_RING_SIZE - 1, n_outputs=len(dest[i])+1, extra_size=0, bulletproof=True)
			fee = calculate_fee(ring_size=25, bytes_no=bytes_no, fee_multiplier=TRANSFER_PRIORITY)

			fee_for_each_one = floor(fee/len(dest[i]))

			for d in dest[i]:
				d['amount'] = d['amount'] - fee_for_each_one

			for u in uids[i]:
				u['fee'] = fee_for_each_one

			json_data['tx_hash'] = 'TEST'
			transfer_info['transfer'] = {}
			transfer_info['transfer']['timestamp'] = 1536234479
			for j in range(len(dest[i])):
				submit_payment(cur,
								uids[i][j]['uid'],
								dest[i][j]['amount'],
								json_data['tx_hash'],
								transfer_info['transfer']['timestamp'],
								uids[i][j]['fee'])

				message('Pay ' + str(format(int(dest[i][j]['amount'])/1000000000, '.9f')) + \
						' to ' + str(dest[i][j]['address']) + \
						' for user ' + str(uids[i][j]['uid']) + '.')
		else:
			try:
				bytes_no = estimate_rct_tx_size(n_inputs=2, mixin=TRANSFER_RING_SIZE - 1, n_outputs=len(dest[i])+1, extra_size=0, bulletproof=True)
				fee = calculate_fee(ring_size=25, bytes_no=bytes_no, fee_multiplier=TRANSFER_PRIORITY)

				fee_for_each_one = floor(fee/len(dest[i]))

				for d in dest[i]:
					d['amount'] = d['amount'] - fee_for_each_one

				for u in uids[i]:
					u['fee'] = fee_for_each_one

				if uids[i][0]['payment_id'] == '':
					json_data = wallet_rpc('transfer',
											{'destinations': dest[i], \
												'priority': TRANSFER_PRIORITY, \
												'mixin': TRANSFER_RING_SIZE - 1}) # 'get_tx_key': True
				else:
					json_data = wallet_rpc('transfer',
											{'destinations': dest[i], \
												'payment_id': uids[i][0]['payment_id'], \
												'priority': TRANSFER_PRIORITY, \
												'mixin': TRANSFER_RING_SIZE - 1}) # 'get_tx_key': True

				transfer_info = wallet_rpc('get_transfer_by_txid', {'txid': json_data['tx_hash']})

				with open('fee', 'a+') as f:
					f.writelines('Calculated: ' + str(fee) + '\n')
					f.writelines('Actual: ' + str(transfer_info['transfer']['fee']) + '\n')
					f.writelines('Diff: ' + str(fee-transfer_info['transfer']['fee']) + '\n\n\n')

				for j in range(len(dest[i])):
					result = submit_payment(cur, 
											uids[i][j]['uid'],
											dest[i][j]['amount'], 
											json_data['tx_hash'],
											transfer_info['transfer']['timestamp'],
											uids[i][j]['fee'])

					if result == True:
						message('Pay ' + str(format(int(dest[i][j]['amount'])/1000000000, '.9f')) + \
								' to ' + str(dest[i][j]['address']) + \
								' for user ' + str(uids[i][j]['uid']) + '.')
					else:
						error('Failed to record payment ' + str(format(int(dest[i][j]['amount'])/1000000000, '.9f')) + \
								' to ' + str(dest[i][j]['address']) + \
								' for user ' + str(uids[i][j]['uid']) + '.')

			except RuntimeError:
				for j in range(len(dest[i])):
					error('Failed to pay ' + str(format(int(dest[i][j]['amount'])/1000000000, '.9f')) + \
							' to ' + str(dest[i][j]['address']) + \
							' for user ' + str(uids[i][j]['uid']) + '.')
		time.sleep(1)

def get_wallet_height():
	"""Get the wallet's current block height"""
	return wallet_rpc('getheight')['height']

try:
	message('Hello!')
	CONN = None
	CURS = None
	CONN, CURS = connection_init()

	database_init(CURS, CONN)

	wallet_rpc('open_wallet', {'filename': WALLET_NAME, 'password': ''})

	while True:

		message('Block: ' + str(get_wallet_height()))

		update_payment_status(CURS)

		update_block_status(CURS)

		calculate_credit(CURS)

		pay_payments(CURS)

		print()

		time.sleep(PAYMENT_INTERVAL)

except KeyboardInterrupt:
	print()
	message('Bye!!!')
	sys.exit()

except RuntimeError as my_exception:
	error(my_exception)
	traceback.print_exception(*sys.exc_info())

else:
	close_connection(CONN, CURS)
