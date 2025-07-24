import yaml
import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, sum, avg, window, expr, to_timestamp, current_timestamp, unix_timestamp
from pyspark.sql.avro.functions import from_avro
from pyspark.ml import PipelineModel
from pyspark.sql.streaming import StreamingQueryException

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

KAFKA_BOOTSTRAP = config['kafka_bootstrap_servers']
TOPIC = config['topic']
CASSANDRA_KEYSPACE = config['cassandra_keyspace']
CASSANDRA_FRAUD_TABLE = config['cassandra_fraud_table']
CASSANDRA_METRICS_TABLE = config['cassandra_metrics_table']
CASSANDRA_HOST = config['cassandra_host']
CASSANDRA_PORT = config['cassandra_port']
SPARK_JARS = config['spark_jars']

# Initializing SparkSession
spark = SparkSession.builder \
    .appName('FraudDetection') \
    .config('spark.jars', SPARK_JARS) \
    .config('spark.sql.session.timeZone', 'UTC') \
    .config('spark.cassandra.connection.host', CASSANDRA_HOST) \
    .config('spark.cassandra.connection.port', CASSANDRA_PORT) \
    .config('spark.sql.streaming.checkpointLocation', '/tmp/spark_checkpoints') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# Load the KMeans model
try:
    model_path = os.path.join(os.path.dirname(__file__), 'kmeans_model')
    model = PipelineModel.load(model_path)
except Exception as e:
    logger.error(f'Failed to load KMeans model: {e}')
    exit(1)

# Reading from Kafka
raw_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP) \
    .option('subscribe', TOPIC) \
    .option('startingOffsets', 'earliest') \
    .load()

# Deserializing Avro (skip the first 5 bytes of magic byte + schema id)
avro_bytes = raw_df.selectExpr('CAST(key AS STRING) AS key', 'substring(value, 6) AS avro_payload')

decoded_df = avro_bytes.select(
    col('key').cast('string'),
    from_avro(col('avro_payload'), schema_str).alias('data')
).filter(col('data').isNotNull())

# Extracting fields
df = decoded_df \
    .withColumn('is_failed_transaction', when(col('data.transaction_status')=='failed', 1).otherwise(0)) \
    .select(
    col('data.transaction_id').alias('transaction_id'),
    to_timestamp(col('data.transaction_timestamp')).alias('transaction_timestamp'),
    col('data.transaction_amount').alias('transaction_amount'),
    col('data.sender_account').alias('sender_account'),
    col('data.receiver_account').alias('receiver_account'),
    col('data.transaction_type').alias('transaction_type'),
    col('data.transaction_status').alias('transaction_status'),
    col('is_failed_transaction')
).withWatermark('transaction_timestamp', '10 seconds')

# Preprocessing stream df 
def preprocessing(df):
    windowed_metrics = df \
        .groupBy(
            window(col('transaction_timestamp'), '1 minute', '30 seconds').alias('metric_window'),
            col('sender_account')
        ) \
        .agg(
            count('*').alias('window_transactions_count'),
            sum('transaction_amount').alias('window_total_amount'),
            sum('is_failed_transaction').alias('window_failed_transactions'),
            expr('collect_list(transaction_id)').alias('transactions_ids')
        )
    window_df = windowed_metrics \
        .withColumn('transaction_id', expr('explode(transactions_ids)')) \
        .select('transaction_id', 'sender_account', 'window_transactions_count', 'window_total_amount', 'window_failed_transactions')
    return window_df

# Applying the KMeans model
def modeling(window_df):
    predictions = model.transform(window_df)
    predictions = predictions.select(col('transaction_id'), col('prediction'), current_timestamp().alias('alert_timestamp'))
    return predictions

# Calculating batch-level metrics
def calculate_batch_metrics(batch_df, predictions):
    df = batch_df.join(predictions, 'transaction_id', 'inner')
    normal = df.filter(col('prediction')==0)
    fraud = df.filter(col('prediction')==1)
    
    total_transactions_count = df.count()
    fraud_transactions_count = fraud.count()
    total_detection_time = fraud.withColumn('time_diff', unix_timestamp('alert_timestamp') - unix_timestamp('transaction_timestamp')).agg(sum('time_diff').alias('total_diff')).collect()[0]['total_diff']
    total_normal_amount = normal.agg(sum('transaction_amount').alias('total_amount')).collect()[0]['total_amount']
    total_fraud_amount = fraud.agg(sum('transaction_amount').alias('total_amount')).collect()[0]['total_amount']
    normal_failed_count = normal.filter(col('is_failed_transaction')==1).count()
    fraud_failed_count = fraud.filter(col('is_failed_transaction')==1).count()
    
    data = [(
        int(total_transactions_count),
        int(fraud_transactions_count),
        int(total_detection_time),
        int(total_normal_amount),
        int(total_fraud_amount),
        int(normal_failed_count),
        int(fraud_failed_count)
        )]
    schema = '''
        total_transactions_count INT,
        fraud_transactions_count INT,
        total_detection_time INT,
        total_normal_amount INT,
        total_fraud_amount INT,
        normal_failed_count INT,
        fraud_failed_count INT
        '''
    metrics_df = spark.createDataFrame(data, schema) \
        .withColumn('updated_at', current_timestamp())
    return metrics_df

# Write fraud_df to Cassandra
def write_fraud_to_cassandra(fraud_df):
    if not fraud_df.isEmpty():
        fraud_df.write \
            .format('org.apache.spark.sql.cassandra') \
            .options(table=CASSANDRA_FRAUD_TABLE, keyspace=CASSANDRA_KEYSPACE) \
            .mode('append') \
            .save()
    else:
        logger.info('No fraudulent transactions in this batch.')
        
# Write metrics_df to Cassandra
def write_metrics_to_cassandra(metrics_df):
    if not metrics_df.isEmpty():
        metrics_df.write \
            .format('org.apache.spark.sql.cassandra') \
            .options(table=CASSANDRA_METRICS_TABLE, keyspace=CASSANDRA_KEYSPACE) \
            .mode('append') \
            .save()
    else:
        logger.info('No metrics in this batch.')
    
# Processing each batch
def process_batch(batch_df, batch_id):
    window_df = preprocessing(batch_df)
    predictions = modeling(window_df)
    fraud_df = predictions.filter(col('prediction')==1) \
        .select(col('transaction_id'), col('alert_timestamp'))
    metrics_df = calculate_batch_metrics(batch_df, predictions)

    write_fraud_to_cassandra(fraud_df)
    write_metrics_to_cassandra(metrics_df)

query = df \
    .writeStream \
    .outputMode('append') \
    .foreachBatch(process_batch) \
    .trigger(availableNow=True) \
    .option('checkpointLocation', '/tmp/spark_checkpoints') \
    .start()

try:
    query.awaitTermination()
except StreamingQueryException as e:
    logger.error(f'Spark streaming query terminated with error: {e}')
except KeyboardInterrupt:
    logger.warning('Spark streaming query shutting down...')
finally:
    if query.isActive:
        query.stop()
        logger.info('Spark streaming query stopped.')
    spark.stop()
    logger.info('Spark session stopped.')
