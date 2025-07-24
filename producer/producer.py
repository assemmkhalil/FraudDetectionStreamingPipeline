import uuid
import random
import time
from datetime import datetime, timezone
import yaml
import logging
import os
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField

# Configuration
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

try:
    config_path = os.path.join(os.path.dirname(__file__), 'config.yml')
    with open(config_path, 'r') as config_file:
        config = yaml.safe_load(config_file)
    schema_path = os.path.join(os.path.dirname(__file__), 'schema.avsc')
    with open(schema_path, 'r') as schema_file:
        schema_str = schema_file.read()
except FileNotFoundError as e:
    logger.error(f'Configuration or schema file not found: {e}')
    exit(1)
except yaml.YAMLError as e:
    logger.error(f'Error parsing configuration file: {e}')
    exit(1)

SCHEMA_REGISTRY_URL = config['schema_registry_url']
KAFKA_BOOTSTRAP_SERVERS = config['kafka_bootstrap_servers']
TOPIC = config['topic']
TRANSACTION_TYPES = ['transfer', 'payment', 'deposit', 'purchase', 'donation']
TYPES_WEIGHTS = [0.3, 0.25, 0.15, 0.2, 0.1]
STATUS_OPTIONS = ['completed', 'failed']
STATUS_WEIGHTS = [0.95, 0.05]  # 5% of transactions will fail

# Initializing clients
try:
    schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(schema_registry_client, schema_str)
    string_serializer = StringSerializer('utf-8')
    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
except Exception as e:
    logger.error(f'Failed to initialize clients: {e}')
    exit(1)

burst_state = {'active': False, 'sender_account': None, 'remaining_transactions': 0}

def generate_transaction():
    '''
    Generate a random financial transaction with controlled fraud patterns
    '''
    transaction_id = str(uuid.uuid4())
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')
    amount = round(random.uniform(10.0, 5000.0), 2)
    sender_account = f'account-{random.randint(1, 100000):06d}'
    receiver_account = f'account-{random.randint(1, 100000):06d}'
    transaction_type = random.choices(TRANSACTION_TYPES, weights=TYPES_WEIGHTS, k=1)[0]
    status = random.choices(STATUS_OPTIONS, weights=STATUS_WEIGHTS, k=1)[0]

    # Simulate anomalies
    chance = random.random()
    # initialize transactions burst only 2% of the time and only if we are not in the middle of another burst
    if chance < 0.02 and not burst_state['active']:
        burst_state['active'] = True
        burst_state['sender_account'] = sender_account
        burst_state['remaining_transactions'] = random.randint(3, 6)
    if burst_state['active']:
        sender_account = burst_state['sender_account']  # keep sender account the same
        burst_state['remaining_transactions'] -= 1
        if burst_state['remaining_transactions'] == 0:
            burst_state['active'] = False

    # for 3% of transactions, increase amount range
    if chance < 0.03:
        amount = round(random.uniform(5001.0, 10000.0), 2)

    # ensure sender and receiver accounts are not the same
    while sender_account == receiver_account:
        receiver_account = f'account-{random.randint(1, 100000):06d}'

    return {
        'transaction_id': transaction_id,
        'transaction_timestamp': timestamp,
        'transaction_amount': amount,
        'sender_account': sender_account,
        'receiver_account': receiver_account,
        'transaction_type': transaction_type,
        'transaction_status': status
    }

def delivery_report(err, msg):
    '''Callback for message delivery status'''
    if err:
        print(f'Delivery failed for {msg.key()}: {err}')
    else:
        print(f'Delivered to {msg.topic()}')

# Produce transactions
if __name__ == '__main__':
    try:
        while True:
            try:
                transaction = generate_transaction()
                key = string_serializer(transaction['transaction_id'], SerializationContext(TOPIC, MessageField.KEY))
                value = avro_serializer(transaction, SerializationContext(TOPIC, MessageField.VALUE))
                producer.produce(TOPIC, key=key, value=value, callback=delivery_report)
                producer.poll(0)
                time.sleep(random.uniform(0, 0.0001))  # variable delay for realism
            except BufferError:
                logger.warning('Buffer full, retrying after 1 second.')
                producer.poll(1)
            except KafkaException as e:
                logger.warning(f'Kafka error during production: {e}')
                time.sleep(1)
            except Exception as e:
                logger.warning(f'Error producing message: {e}')
                time.sleep(1)
    except KeyboardInterrupt:
        logger.warning('Producer shutting down, flushing remaining messages...')
    finally:
        producer.flush(timeout=10)
        logger.warning('Producer flushed.')

