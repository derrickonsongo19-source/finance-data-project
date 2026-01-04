from kafka import KafkaConsumer
import json
from datetime import datetime
import time

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'financial_transactions',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='finance-analytics-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Initialize data structures for real-time analytics
account_balances = {}
fraud_alerts = []
spending_patterns = {}

print("🔥 Starting Real-Time Finance Analytics Consumer")
print("Listening for transactions...\n")

def detect_fraud(transaction):
    """Fraud detection logic"""
    fraud_score = 0
    reasons = []
    
    # Rule 1: Large international transactions
    if transaction['amount'] < -300 and transaction['location'] == 'INTERNATIONAL':
        fraud_score += 85
        reasons.append("Large international transaction")
    
    # Rule 2: Unusual hour transactions
    tx_hour = datetime.fromisoformat(transaction['timestamp']).hour
    if transaction['amount'] < -200 and tx_hour < 6:
        fraud_score += 70
        reasons.append("Unusual hour transaction (late night)")
    
    # Rule 3: Large amount in discretionary categories
    if abs(transaction['amount']) > 500 and transaction['category'] in ['Food', 'Entertainment']:
        fraud_score += 60
        reasons.append("Large discretionary spending")
    
    # Rule 4: Already flagged as fraudulent
    if transaction.get('is_fraudulent', False):
        fraud_score = 100
        reasons.append("Pre-flagged as fraudulent")
    
    return fraud_score, reasons

def update_account_balance(transaction):
    """Update real-time account balances"""
    account = transaction['account_id']
    amount = transaction['amount']
    
    if account not in account_balances:
        account_balances[account] = 10000  # Starting balance
    
    account_balances[account] += amount
    return account_balances[account]

def detect_anomaly(transaction):
    """Anomaly detection for spending patterns"""
    account = transaction['account_id']
    category = transaction['category']
    
    key = f"{account}_{category}"
    
    if key not in spending_patterns:
        spending_patterns[key] = {
            'count': 0,
            'total': 0,
            'timestamps': []
        }
    
    pattern = spending_patterns[key]
    pattern['count'] += 1
    pattern['total'] += transaction['amount']
    pattern['timestamps'].append(datetime.fromisoformat(transaction['timestamp']))
    
    # Keep only last 10 transactions (sliding window)
    if len(pattern['timestamps']) > 10:
        pattern['timestamps'] = pattern['timestamps'][-10:]
        pattern['count'] = min(pattern['count'], 10)
    
    # Check for anomalies
    anomalies = []
    if pattern['count'] >= 5 and pattern['total'] / pattern['count'] < -200:
        anomalies.append(f"High frequency large spending in {category}")
    
    return anomalies

# Process messages
transaction_count = 0
try:
    for message in consumer:
        transaction = message.value
        transaction_count += 1
        
        print(f"\n📊 Transaction #{transaction_count}")
        print(f"   ID: {transaction['transaction_id']}")
        print(f"   Account: {transaction['account_id']}")
        print(f"   Amount: ${transaction['amount']:.2f}")
        print(f"   Category: {transaction['category']}")
        
        # 1. Fraud Detection
        fraud_score, fraud_reasons = detect_fraud(transaction)
        if fraud_score > 50:
            print(f"   ⚠️  FRAUD RISK: {fraud_score}/100")
            for reason in fraud_reasons:
                print(f"      - {reason}")
            fraud_alerts.append({
                'transaction': transaction['transaction_id'],
                'score': fraud_score,
                'reasons': fraud_reasons,
                'timestamp': datetime.now().isoformat()
            })
        
        # 2. Real-time Balance Update
        new_balance = update_account_balance(transaction)
        print(f"   💰 Updated Balance: ${new_balance:.2f}")
        
        # 3. Anomaly Detection
        anomalies = detect_anomaly(transaction)
        if anomalies:
            print(f"   🔍 ANOMALY DETECTED:")
            for anomaly in anomalies:
                print(f"      - {anomaly}")
        
        # 4. Periodic summary
        if transaction_count % 10 == 0:
            print(f"\n{'='*50}")
            print(f"📈 REAL-TIME SUMMARY (Last {transaction_count} transactions)")
            print(f"   Active accounts: {len(account_balances)}")
            print(f"   Fraud alerts: {len(fraud_alerts)}")
            print(f"   Current balances:")
            for acc, bal in list(account_balances.items())[:3]:  # Show top 3
                print(f"      - {acc}: ${bal:.2f}")
            print(f"{'='*50}\n")
        
except KeyboardInterrupt:
    print(f"\n\n🛑 Stopped. Processed {transaction_count} transactions")

finally:
    consumer.close()
    print("✅ Consumer closed")
    
    # Final summary
    print("\n🎯 FINAL REAL-TIME ANALYTICS:")
    print(f"   Total transactions: {transaction_count}")
    print(f"   Accounts monitored: {len(account_balances)}")
    print(f"   Fraud alerts triggered: {len(fraud_alerts)}")
    
    if fraud_alerts:
        print("\n   Top fraud alerts:")
        for alert in fraud_alerts[-3:]:  # Last 3 alerts
            print(f"      - {alert['transaction']}: Score {alert['score']}")
