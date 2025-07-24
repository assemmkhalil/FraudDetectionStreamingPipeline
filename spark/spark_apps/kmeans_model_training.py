import plotly.express as px
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, sum, round, window, current_timestamp, expr
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml import Pipeline

spark = SparkSession.builder.appName('InitialKMeansModel').getOrCreate()

schema = '''
transaction_id STRING,
transaction_timestamp TIMESTAMP,
transaction_amount DOUBLE,
sender_account STRING,
receiver_account STRING,
transaction_type STRING,
transaction_status STRING
'''
df = spark.read.csv('train_df.csv', header=True, schema=schema) \
    .withColumn('is_failed_transaction', when(col('transaction_status')=='failed', 1).otherwise(0))

windowed_metrics = df \
    .groupBy(
        window(col('transaction_timestamp'), '1 minute').alias('metric_window'),
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
    .select('transaction_id', 'window_transactions_count', 'window_total_amount', 'window_failed_transactions')

assembler = VectorAssembler(
    inputCols=['window_transactions_count', 'window_total_amount', 'window_failed_transactions'],
    outputCol='features'
)
scaler = StandardScaler(inputCol='features', outputCol='scaled_features')
kmeans = KMeans(featuresCol='scaled_features', k=2, seed=1)  # only two clusters (normal/fraud)
pipeline = Pipeline(stages=[assembler, scaler, kmeans])
model = pipeline.fit(window_df)
predictions = model.transform(window_df)
model.save('kmeans_model')

evaluator = ClusteringEvaluator(featuresCol='scaled_features', predictionCol='prediction', metricName='silhouette')
silhouette_score = evaluator.evaluate(predictions)
print(f'Silhouette Score: {silhouette_score}')

clusters = predictions.groupBy(col('prediction').alias('cluster')).agg(count('*').alias('transactions'))
clusters = clusters.withColumn('percentage', round((clusters.transactions / predictions.count() * 100), 2))
clusters.show()

fig = px.scatter_3d(
    predictions, x='window_transactions_count', y='window_failed_transactions', z='window_total_amount', 
    color='prediction', 
    title='KMeans Clusters',
    labels={'color': 'Cluster'},
    opacity=0.7
)
fig.write_html('fig.html')
