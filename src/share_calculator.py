import sys
import os
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


WORKING_HIGHT = 160000
SLEEP_TIME = 0.5
BLOCK_REWARD = 41
POOL_FEE = 0


def message(string):
	string = str(string)
	print('[' + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + '] '\
	+ Fore.GREEN + 'Message: ' + Fore.RESET + string)

def error(string):
	string = str(string)
	print('[' + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + '] '\
	+ Fore.RED + 'Error: ' + Fore.RESET + string)


try:
	CONN = psycopg2.connect(\
				user=SETTING["psqlUser"], \
				password=SETTING["psqlPass"], \
				host="localhost", \
				port="5432")
	message('Connection created')

	CONN.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
	CURS = CONN.cursor()
	message('Cursor created')

	CURS.execute('DROP SCHEMA IF EXISTS wpv1 cascade')
	CONN.commit()
	message('Schema wpv1 droped')

	with open('CREATE_DB.sql', 'r') as myFile:
		CURS.execute(myFile.read())
	message('Schema wpv1 created')

	with open('DATA.sql', 'r') as myFile:
		CURS.execute(myFile.read())
	message('Data added')

	CURS.execute('SET search_path TO wpv1')
	message('Set search_path to wpv1')


	while True:
		time.sleep(SLEEP_TIME)

		CURS.execute('SELECT status FROM mined_blocks WHERE height=' + str(WORKING_HIGHT))
		RESULT = CURS.fetchone()
		if RESULT is None:
			continue
		RESULT = RESULT[0]

		if RESULT in (0, 1):
			HEADERS = {'Content-Type': 'application/json',}
			DATA = '{"jsonrpc":"2.0","id":"0","method":\
					"get_block","params":{"height":'+str(WORKING_HIGHT)+'}}'
			RESPONSE = requests.post('http://127.0.0.1:12211/json_rpc', headers=HEADERS, data=DATA)

			JSON_DATA = json.loads(RESPONSE.text)

			if 'error' in JSON_DATA:
				raise Exception(JSON_DATA['error']['message'])
			elif 'result' in JSON_DATA:
				CURS.execute('UPDATE mined_blocks SET status=2 WHERE height=' + str(WORKING_HIGHT))
				message('Block ' + str(WORKING_HIGHT) + ' status updated to 2')

				CURS.execute('SELECT status FROM mined_blocks WHERE height=' + str(WORKING_HIGHT - 60))
				RESULT = CURS.fetchone()
				if RESULT is not None:
					CURS.execute('UPDATE mined_blocks SET status=2 WHERE height=' + str(WORKING_HIGHT - 60))
					message('Block ' + str(WORKING_HIGHT - 60) + ' status updated to 3')

					# Calculate credit
					CURS.execute('SELECT time FROM mined_blocks WHERE height=' + str(WORKING_HIGHT - 61))
					TIME_PREV = CURS.fetchone()

					if TIME_PREV is not None:
						TIME_PREV = TIME_PREV[0]

						CURS.execute('SELECT time FROM mined_blocks WHERE height=' + str(WORKING_HIGHT - 60))
						TIME_CURE = CURS.fetchone()[0]

						CURS.execute('SELECT * FROM valid_shares WHERE time BETWEEN ' + str(TIME_PREV + 1) + \
' AND ' + str(TIME_CURE))
						VAILD_SHARES = CURS.fetchall()

						VAILD_SHARES.sort(key=lambda x: int(x[1]))
						USER_TOTAL_VALID_SHARE_IN_BLOCK = []
						TOTAL_VALID_SHARE_IN_BLOCK = 0
						for elt, items in groupby(VAILD_SHARES, itemgetter(1)):
							USER_VALID_SHARES_COUNT = 0
							for i in items:
								USER_VALID_SHARES_COUNT += int(i[3])
							TOTAL_VALID_SHARE_IN_BLOCK += USER_VALID_SHARES_COUNT
							USER_TOTAL_VALID_SHARE_IN_BLOCK.append({'uid': elt, 'valid_shares': USER_VALID_SHARES_COUNT})

						CURS.execute('SELECT blk_id from mined_blocks WHERE height=' + str(WORKING_HIGHT - 60))
						BLK_ID = CURS.fetchone()[0]

						for i in range(len(USER_TOTAL_VALID_SHARE_IN_BLOCK)):
							USER_TOTAL_VALID_SHARE_IN_BLOCK[i]['credit'] = \
							USER_TOTAL_VALID_SHARE_IN_BLOCK[i]['valid_shares'] * \
							(BLOCK_REWARD * (1 - POOL_FEE) / TOTAL_VALID_SHARE_IN_BLOCK)
							CURS.execute('INSERT INTO credits (blk_id, uid, amount) VALUES (' + str(BLK_ID) \
+ ', ' + str(USER_TOTAL_VALID_SHARE_IN_BLOCK[i]['uid']) + ', ' + \
str(USER_TOTAL_VALID_SHARE_IN_BLOCK[i]['credit']) + ')')

						message('Block ' + str(WORKING_HIGHT - 60) + ' valid shares calculated')

				WORKING_HIGHT += 1
		else:
			WORKING_HIGHT += 1

	CURS.execute('select * from mined_blocks')

except KeyboardInterrupt:
	sys.exit()

except Exception as my_exception:
	EXC_TYPE, EXC_OBJ, EXC_TB = sys.exc_info()
	FNAME = os.path.split(EXC_TB.tb_frame.f_code.co_filename)[1]
	error(my_exception)
	print(EXC_TYPE, FNAME, EXC_TB.tb_lineno)
