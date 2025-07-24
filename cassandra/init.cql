CREATE KEYSPACE IF NOT EXISTS demo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE demo;

CREATE TABLE IF NOT EXISTS transactions (
transaction_id TEXT PRIMARY KEY,
transaction_timestamp TIMESTAMP,
transaction_amount DECIMAL,
sender_account TEXT,
receiver_account TEXT,
transaction_type TEXT,
transaction_status TEXT
);

CREATE TABLE IF NOT EXISTS fraud_alerts (
transaction_id TEXT PRIMARY KEY,
alert_timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS metrics (
updated_at TIMESTAMP PRIMARY KEY,
total_transactions_count INT,
fraud_transactions_count INT,
total_detection_time INT,
total_normal_amount INT,
total_fraud_amount INT,
normal_failed_count INT,
fraud_failed_count INT,
);
