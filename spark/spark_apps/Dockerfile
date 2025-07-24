FROM bitnami/spark:3.5.0
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN mkdir -p /tmp/spark_checkpoints && chmod -R 777 /tmp/spark_checkpoints
WORKDIR /opt/spark_apps
CMD ["spark-submit", "--jars", "/opt/jars/*.jar", "fraud_detection.py"]
