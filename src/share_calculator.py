import datetime
import time
import json
import requests

import psycopg2

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from colorama import init as colorama_init, Fore

from settings import setting

colorama_init(autoreset=True)

WORKING_HIGHT = 168821

def message(string):
	print('[' + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + '] '\
	+ Fore.GREEN + 'Message: ' + Fore.RESET + string)
def error(string):
	print('[' + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + '] '\
	+ Fore.RED + 'Error: ' + Fore.RESET + string)

try:
	CONN = psycopg2.connect(\
				user=setting["psqlUser"], \
				password=setting["psqlPass"], \
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
	CURS.execute('SET search_path TO wpv1')
	message('Set search_path to wpv1')

	CURS.execute('select * from mined_blocks')

	HEADERS = {'Content-Type': 'application/json',}
	DATA = '{"jsonrpc":"2.0","id":"0","method":"get_block","params":{"height":'+str(WORKING_HIGHT)+'}}'
	RESPONSE = requests.post('http://127.0.0.1:12211/json_rpc', headers=HEADERS, data=DATA)

	JSON_DATA = json.loads(RESPONSE.text)

	if 'error' in JSON_DATA:
		raise Exception(JSON_DATA['error']['message'])
	elif 'result' in JSON_DATA:
		message(str(JSON_DATA['result']['miner_tx_hash']))

except Exception as my_exception:
	error(str(my_exception))
