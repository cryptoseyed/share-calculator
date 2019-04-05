BEGIN;
CREATE SCHEMA wpv1;
CREATE TABLE wpv1.users (
	uid SERIAL PRIMARY KEY, 
	wallet TEXT NOT NULL UNIQUE, 
	password CHAR(128), 
	auth_miners BOOLEAN NOT NULL DEFAULT FALSE, 
	protect_profile BOOLEAN NOT NULL DEFAULT FALSE, 
	anon_leader BOOLEAN NOT NULL DEFAULT FALSE, 
	email_me BOOLEAN NOT NULL DEFAULT FALSE, 
	email VARCHAR(255), 
	user_diff INTEGER NOT NULL DEFAULT 0,
	payment_threshold BIGINT NOT NULL DEFAULT 1000000000
);
CREATE UNIQUE INDEX wallet_idx ON wpv1.users(wallet);
CREATE TYPE status_setting AS ENUM ('FAILED', 'MONITORED', 'SUCCESS', 'FEE');
CREATE TABLE wpv1.payments (
	pymt_id SERIAL PRIMARY KEY, 
	uid INTEGER NOT NULL, 
	amount BIGINT, 
	txid CHAR(64), 
	time INTEGER, 
	locked BOOLEAN NOT NULL DEFAULT FALSE,
	status status_setting
);
CREATE INDEX payment_idx ON wpv1.payments(uid);
CREATE TABLE wpv1.credits (
	crd_id SERIAL PRIMARY KEY, 
	blk_id INTEGER NOT NULL, 
	uid INTEGER NOT NULL, 
	amount BIGINT, 
	UNIQUE(blk_id, uid)
);
CREATE INDEX credit_idx ON wpv1.credits(uid);
CREATE TABLE wpv1.user_ban (
	uid INTEGER PRIMARY KEY, lift_time INTEGER, 
	msg TEXT
);
CREATE TABLE wpv1.ip_ban (
	ip_regex TEXT PRIMARY KEY, lift_time INTEGER, 
	msg TEXT
);
CREATE TABLE wpv1.rigs (
	rid SERIAL PRIMARY KEY, 
	uid INTEGER NOT NULL, 
	name VARCHAR(255) NOT NULL, 
	UNIQUE(uid, name)
);
CREATE INDEX rig_name_idx ON wpv1.rigs(uid, name);
CREATE TABLE wpv1.valid_shares (
	rid INTEGER NOT NULL, uid INTEGER NOT NULL, 
	time INTEGER NOT NULL, count INTEGER NOT NULL
);
CREATE INDEX valid_shares_by_user_idx ON wpv1.valid_shares(uid);
CREATE INDEX valid_shares_by_time ON wpv1.valid_shares(time DESC);
CREATE TABLE wpv1.reported_hashrate (
	rid INTEGER NOT NULL, uid INTEGER NOT NULL, 
	time INTEGER NOT NULL, hps INTEGER NOT NULL
);
CREATE INDEX reported_hashrate_by_user_idx ON wpv1.reported_hashrate(uid);
CREATE INDEX reported_hashrate_by_time ON wpv1.reported_hashrate(time DESC);
CREATE TABLE wpv1.bad_shares (
	rid INTEGER NOT NULL, uid INTEGER NOT NULL, 
	time INTEGER NOT NULL, count INTEGER NOT NULL
);
CREATE INDEX bad_shares_by_user_idx ON wpv1.bad_shares(uid);
CREATE INDEX bad_shares_by_time ON wpv1.bad_shares(time DESC);
CREATE TABLE wpv1.log (
	uid INTEGER, 
	ip_addr TEXT, 
	time INTEGER, 
	agent VARCHAR(255)
);
CREATE TABLE wpv1.mined_blocks (
	blk_id SERIAL PRIMARY KEY, 
	txid CHAR(64), 
	height INTEGER, 
	time INTEGER, 
	uid INTEGER, 
	status INTEGER
);
CREATE TABLE wpv1.motd (id SERIAL PRIMARY KEY, message TEXT);
COMMIT;
