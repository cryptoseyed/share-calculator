import time
import json
import random
import requests
import csv

from settings import SETTING

MY_FILE = open("DATA.sql", "w+")

with open('blocks.csv', 'w') as block_file:
	share_writer = csv.writer(block_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
	share_writer.writerow(['height', 'difficulty', 'timestamp'])
	for WORKING_HIGHT in range(120000, 120201):
		time.sleep(0.2)

		HEADERS = {'Content-Type': 'application/json',}
		DATA = '{"jsonrpc":"2.0","id":"0","method":\
				"get_block","params":{"height":'+str(WORKING_HIGHT)+'}}'
		RESPONSE = requests.post('http://' + SETTING['SG_DAEMON_ADDR_TESTNET'] + \
									'/json_rpc', headers=HEADERS, data=DATA)

		JSON_DATA = json.loads(RESPONSE.text)

		if 'result' in JSON_DATA:
			print(JSON_DATA['result']['miner_tx_hash'])
			MY_FILE.write('INSERT into wpv1.mined_blocks (txid, height, time, status) \
	VALUES (\'' + JSON_DATA['result']['miner_tx_hash'] + '\', ' + str(WORKING_HIGHT) + ', ' + \
	str(JSON_DATA['result']['block_header']['timestamp']) + ', 3);\n')
			share_writer.writerow([JSON_DATA['result']['block_header']['height'],\
									JSON_DATA['result']['block_header']['difficulty'],\
										JSON_DATA['result']['block_header']['timestamp']])

MY_FILE.write('COMMIT;\n')

with open('shares.csv', 'w') as share_file:
	share_writer = csv.writer(share_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
	share_writer.writerow(['rid', 'uid', 'time', 'count'])
	for i in range(10000):
		RID = random.randint(1, 5)
		UID = random.randint(1, 30)
		TIME = random.randint(1527131460, 1527181566)
		COUNT = random.randint(1, 3000)
		MY_FILE.write('INSERT INTO wpv1.valid_shares (rid, uid, time, count) VALUES (' \
	+ str(RID) + ', ' + str(UID) + ', ' + str(TIME) + ', ' + str(COUNT) + ');\n')
		share_writer.writerow([RID, UID, TIME, COUNT])
		print('Random share No. ' + str(i+1) + ' done')

MY_FILE.write('COMMIT;\n')

MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (0, \'\
RYoHtNo3BGZNFas84dQpyaW7C1Ctorkz4EHMZf4fESEiAQBnnBSLhNr\', 10000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (1, \'\
RYoHtLHM1EHhEjBDBkCFpzMFJHSF7NG3b7WQWjM5ErRRT2Ly7DUZ4tG\', 10000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (2, \'\
RYoHtRbZaDziqyJf8CpkL8cfgusLmzuxvN9gLwd8GvtPc494Hdpygha\', 10000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (3, \'\
RYoHtPRCtgQ8pNFtwpt3dAj1ueZ2M85WLTLRhGdp325bT3PmcWHCLyx\', 10000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (4, \'\
RYoTr2XRA4qbX5T8tEeewJPwdYrc88qNo4qxhX4iciDtB7tiQ5taCzCA5wYcHdxtaT34R5Y6e7n1hj4sw39SNjtQ29iXb58Prec\', 10000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (5, \'\
RYoUqkgswyMS2DbwZNancZCse6fBHevFYYJxjYbXjmnFXYQqTtgcyKvWtscvr24B7rEFMyhs47vfETfJ6AY5XcyAg5RcuD1RYcq\', 10000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (6, \'\
RYoEu61QNLhbX5T8tEeewJPwdYrc88qNo4qxhX4iciDtB7tiQ5taCzCA5wYcHdxtaT34R5Y6e7n1hj4sw39SNjtQ29iXb3HdSKD1qjcTShZux2\', 10000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (7, \'\
RYoTr2XRA4qbX5T8tEeewJPwdYrc88qNo4qxhX4iciDtB7tiQ5taCzCA5wYcHdxtaT34R5Y6e7n1hj4sw39SNjtQ29iXb58Prec.0102030405060708111213141516171821222324252627283132333435363738\', 10000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (8, \'\
RYoTqyZe1acDsEdLXER6oj8yBXEaYjez1dY7iTJV9x5UEg31B2SQBPbToZu7qN4c2pBJBNZ5kLi18EkJYWiSznnPFWvuPVzJgqj\', 10000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (9, \'\
RYoUqnfP7QGaGzwqvkiRvfiA8AsEkRYXsAhs5P5RTvhJU612b8GjP9NSQaJpYyHjhpDuQA53HCa1VCTVoTXeMJeXRjzUJC8T39M\', 10000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (10, \'\
RYoEu33dDrUDsEdLXER6oj8yBXEaYjez1dY7iTJV9x5UEg31B2SQBPbToZu7qN4c2pBJBNZ5kLi18EkJYWiSznnPFWvuPR25nxB1qjcTPf2aJp\', 10000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (11, \'\
RYoTqyZe1acDsEdLXER6oj8yBXEaYjez1dY7iTJV9x5UEg31B2SQBPbToZu7qN4c2pBJBNZ5kLi18EkJYWiSznnPFWvuPVzJgqj.0102030405060708111213141516171821222324252627283132333435363738\', 10000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (12, \'\
RYoTqyUJGj2UsrjiSuzFSrXoet2bdaYkiSM2VYYv4Ebe4ozPjKAKtyUHEUXiwcWy5X8zq84jJCSkEcP1DxoA9na7BptetDo1RYf\', 10000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (13, \'\
RYoUqknfQqGTU9Rbh4NjERWKAeQa7MRGe8D8AkhNAeQs9gnCUskggskdf7EhcQTSNgd5sTATogQz9GdmGHqcWf$ZYNQRM86UuH6\', 10000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (14, \'\
RYoEu2xHUztUsrjiSuzFSrXoet2bdaYkiSM2VYYv4Ebe4ozPjKAKtyUHEUXiwcWy5X8zq84jJCSkEcP1DxoA9n$7BptetDPj9dD1qjcTM44jvr\', 10000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (15, \'\
RYoTqyUJGj2UsrjiSuzFSrXoet2bdaYkiSM2VYYv4Ebe4ozPjKAKtyUHEUXiwcWy5X8zq84jJCSkEcP1DxoA9n$7BptetDo1RYf.0102030405060708111213141516171821222324252627283132333435363738\', 10000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (16, \'\
RYoTqzs1o6kNQx33aofyxQcMKZxUTQCD63BJ9NRZv7pySpYDEU3hevySdmZdjqKP5z81K8dPxVFun9zu9TiUy2$ed9VN8mubbVG\', 10000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (17, \'\
RYoUqkjo9C44ivLiXruauVED2tzkSdazKLrwm8qDELKXH9QsvmCh5bdgqFuPu8xX9NApjaFFCAPHa23LzuGmzY$xJ2jRB7W45Ss\', 10000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (18, \'\
RYoEu4M11NcNQx33aofyxQcMKZxUTQCD63BJ9NRZv7pySpYDEU3hevySdmZdjqKP5z81K8dPxVFun9zu9TiUy2Ned9VN8hknMw11qjcTQSDPe5\', 10000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (19, \'\
RYoTqzs1o6kNQx33aofyxQcMKZxUTQCD63BJ9NRZv7pySpYDEU3hevySdmZdjqKP5z81K8dPxVFun9zu9TiUy2Ned9VN8mubbVG.0102030405060708111213141516171821222324252627283132333435363738\', 10000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (20, \'\
RYoTr3Mhkb8VRDVNjwBdqCWXGjN8TrjJ3gdCkuZ58Roph4BxMqYhHqAT238XbUQ3YtEThaiMrGLCH9LyttMTAT1CTstbgL2XFWk\', 10000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (21, \'\
RYoUqkMtfYyCpU5p3pba1hUm3BUsWshh5K7w3VB6UQCDPhhrXVtYTRuc7gH939HTTgURtGELfAYCyUPviNBxrThidhcnciwkGB9\', 10000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (22, \'\
RYoEu6qgxrzVRDVNjwBdqCWXGjN8TrjJ3gdCkuZ58Roph4BxMqYhHqAT238XbUQ3YtEThaiMrGLCH9LyttMTAT1CTstbgGBuRfh1qjcTNb9p7c\', 10000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (23, \'\
RYoTr3Mhkb8VRDVNjwBdqCWXGjN8TrjJ3gdCkuZ58Roph4BxMqYhHqAT238XbUQ3YtEThaiMrGLCH9LyttMTAT1CTstbgL2XFWk.0102030405060708111213141516171821222324252627283132333435363738\', 10000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (24, \'\
RYoTr56NHyX1JwK3esHZRBfPpkd8D7sSHArEi9ZbQy1XiWWLyT5BSY7BZAsh6wgmDVXrVQrZHKRyN87HQz6Y4ffJiGMhRanGvHC\', 10000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (25, \'\
RYoUqqpJZyfT4xqomfrUshFxq889S6Dt26fLA94tGLhDNZpCPun7VqoRNaKKrGP88LAipPNCBJcYQNn1NB3Uu5Gp8TcGtdgA6Zh\', 10000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (26, \'\
RYoUqnDgQHFDZGucrBHfgMioPtgoPxU9KFPWRHUniptYdENPeW7HRkUTHgaMkLSVGgDV7T3xCkADw2fffiYdQwv4ekNotcVAMYK\', 10000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (27, \'\
RYoUqopdAsp1bFQzWR5uTVAzYXKSJHUKy9fJuRz8m6eDjXYVpYpR8dcbPVzhLp3K9cHUXpPeUPx8YadEcnviCZG6PnsVqLAQ6eZ\', 10000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (28, \'\
RYoUqnKN4ELSsUoMskN4GsFGxGCu99EW37MbkUqV2TUnMoVY3T3N1gZPcQAsxEJ2rPYkoXziEm1HyQMLkZfvbBMmKk6BP66xhmV\', 10000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (29, \'\
RYoUqptYqdVUM61QbQnJgXRC8USfqy1QpZJefJwVygKa5RBaBAp3qS9gkXAjoGcKnBJP6E2DXhL36TMGRmFtYcBKeFJ3P1zhyfK\', 10000000);\n')
MY_FILE.write('INSERT INTO wpv1.users (uid, wallet, payment_threshold) VALUES (30, \'\
RYoUqpTyZPR849nw2hL9pY2EuB5hAXRcM7LCH2nHLMyFWtY4BXCSG84aeoXPzsjLt31rzSTsoLow414z5iCDV1DvbNj2jC5Lenf\', 10000000);\n')

MY_FILE.write('COMMIT;\n')

print('User data added')

MY_FILE.close()
