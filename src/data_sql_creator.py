import time
import json
import random
import requests

MY_FILE = open("DATA.sql", "w+")

for WORKING_HIGHT in range(160000, 160901):
	time.sleep(0.1)

	HEADERS = {'Content-Type': 'application/json',}
	DATA = '{"jsonrpc":"2.0","id":"0","method":\
			"get_block","params":{"height":'+str(WORKING_HIGHT)+'}}'
	RESPONSE = requests.post('http://127.0.0.1:12211/json_rpc', headers=HEADERS, data=DATA)

	JSON_DATA = json.loads(RESPONSE.text)

	if 'result' in JSON_DATA:
		print(JSON_DATA['result']['miner_tx_hash'])
		MY_FILE.write('INSERT into wpv1.mined_blocks (txid, height, time, status) \
VALUES (\'' + JSON_DATA['result']['miner_tx_hash'] + '\', ' + str(WORKING_HIGHT) + ', ' + \
str(JSON_DATA['result']['block_header']['timestamp']) + ',1);\n')

MY_FILE.write('COMMIT;\n')

for i in range(10000):
	RID = random.randint(1, 5)
	UID = random.randint(1, 3)
	TIME = random.randint(1533413533, 1533437454)
	COUNT = random.randint(1, 3000)
	MY_FILE.write('INSERT INTO wpv1.valid_shares (rid, uid, time, count) VALUES (' \
+ str(RID) + ', ' + str(UID) + ', ' + str(TIME) + ', ' + str(COUNT) + ');\n')
	print('Random share No. ' + str(i+1) + ' done')

MY_FILE.write('COMMIT;\n')

MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (0, \'\
RYoHtNo3BGZNFas84dQpyaW7C1Ctorkz4EHMZf4fESEiAQBnnBSLhNr\', 100000000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (1, \'\
RYoHtLHM1EHhEjBDBkCFpzMFJHSF7NG3b7WQWjM5ErRRT2Ly7DUZ4tG\', 2000000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (2, \'\
RYoHtRbZaDziqyJf8CpkL8cfgusLmzuxvN9gLwd8GvtPc494Hdpygha\', 200000000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (3, \'\
RYoHtPRCtgQ8pNFtwpt3dAj1ueZ2M85WLTLRhGdp325bT3PmcWHCLyx\', 50000000000);\n')

MY_FILE.write('COMMIT;\n')

print('User data added')

MY_FILE.close()
