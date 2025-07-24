import time
import logging
from flask import Flask, render_template, jsonify
from cassandra.cluster import Cluster

# Initialization
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# Cassandra connection
for i in range(10):
    try:
        cluster = Cluster(['cassandra'], port=9042)
        session = cluster.connect('demo')
        break
    except:
        logging.warning('Cassandra not ready, retrying...')
        time.sleep(5)
    finally:
        if not session:
            logging.error('Could not connect to Cassandra')
            exit(1)

@app.route('/')
def index():
    '''
    Renders the main dashboard HTML page.
    '''
    return render_template('index.html')

@app.route('/api/summary')
def api_summary():
    '''
    API endpoint to return real-time metrics as JSON.
    '''
    try:
        query = '''
        SELECT
               SUM(total_transactions_count) as total_transactions,
               SUM(fraud_transactions_count) as fraud_transactions,
               SUM(total_detection_time) as detection_time,
               SUM(total_normal_amount) as normal_amount,
               SUM(total_fraud_amount) as fraud_amount,
               SUM(normal_failed_count) as normal_failed,
               SUM(fraud_failed_count) as fraud_failed,
               MAX(updated_at) as updated
        FROM metrics
        '''
        result = session.execute(query).one()
        
        if result:
            total_transactions_count = int(result.total_transactions or 0)
            fraud_transactions_count = int(result.fraud_transactions or 0)
            total_detection_time = int(result.detection_time or 0)
            total_normal_amount = int(result.normal_amount or 0)
            total_fraud_amount = int(result.fraud_amount or 0)
            normal_failed_count = int(result.normal_failed or 0)
            fraud_failed_count = int(result.fraud_failed or 0)
            updated_at = result.updated
            
            normal_transactions_count = total_transactions_count - fraud_transactions_count
            fraud_transactions_pct = (fraud_transactions_count / total_transactions_count) * 100 if total_transactions_count > 0 else 0
            avg_detection_time = total_detection_time / fraud_transactions_count if fraud_transactions_count > 0 else 0
            avg_normal_amount = total_normal_amount / normal_transactions_count if normal_transactions_count > 0 else 0
            avg_fraud_amount = total_fraud_amount / fraud_transactions_count if fraud_transactions_count > 0 else 0
            normal_failed_pct = (normal_failed_count / normal_transactions_count) * 100 if normal_transactions_count > 0 else 0
            fraud_failed_pct = (fraud_failed_count / fraud_transactions_count) * 100 if fraud_transactions_count > 0 else 0
            
            response_data = {
                'total_transactions_count': total_transactions_count,
                'fraud_transactions_count': fraud_transactions_count,
                'fraud_transactions_pct': fraud_transactions_pct,
                'avg_detection_time': avg_detection_time,
                'avg_normal_amount': avg_normal_amount,
                'avg_fraud_amount': avg_fraud_amount,
                'normal_failed_pct': normal_failed_pct,
                'fraud_failed_pct': fraud_failed_pct,
                'updated_at': updated_at if updated_at else 'N/A'
                }
            return jsonify(response_data)
            logging.info('Fetched metrics for API.')
            
        else:
            logging.warning('No metrics found in Cassandra.')
            return jsonify({'message': 'No metrics available yet.', 'metrics': {}}), 200

    except Exception as e:
        logging.error(f'Error fetching metrics from Cassandra: {e}')
        return jsonify({'error': f'Could not fetch metrics: {e}.'}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
