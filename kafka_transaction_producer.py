from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime, timedelta

# Kafka configuration
bootstrap_servers = 'localhost:9092'
transaction_topic = 'financial_transactions'

# Sample data
accounts = ['Checking_1234', 'Savings_5678', 'CreditCard_9012', 'Business_3456']
categories = ['Food', 'Shopping', 'Transport', 'Entertainment', 'Bills', 'Healthcare', 'Income', 'Transfer']
merchants = ['Amazon', 'Walmart', 'Starbucks', 'Uber', 'Netflix', 'Apple', 'Google', 'Local Market']

# Initialize producer
producer = KafkaProducer(
    bootstrap_servers=[bootstrap_servers],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: v.encode('utf-8') if v else None
)

def generate_transaction():
    """Generate a random financial transaction"""
    amount = random.uniform(-500, 2000)  # Negative for expenses, positive for income
    is_fraudulent = random.random() < 0.05  # 5% chance of fraud
    
    transaction = {
        'transaction_id': f'TXN{random.randint(10000, 99999)}',
        'account_id': random.choice(accounts),
        'amount': round(amount, 2),
        'category': random.choice(categories),
        'merchant': random.choice(merchants),
        'timestamp': (datetime.now() - timedelta(minutes=random.randint(0, 60))).isoformat(),
        'location': f'{random.choice(["NY", "CA", "TX", "FL"])}_{random.randint(100, 999)}',
        'is_fraudulent': is_fraudulent,
        'description': f'Payment to {random.choice(merchants)}' if amount < 0 else f'Deposit from {random.choice(["Employer", "Client", "Transfer"])}'
    }
    
    # Add suspicious patterns for fraud
    if is_fraudulent:
        transaction['amount'] = round(random.uniform(-1000, -300), 2)  # Large negative
        transaction['location'] = 'INTERNATIONAL'  # Foreign transaction
    
    return transaction

def send_transaction():
    """Send a transaction to Kafka"""
    transaction = generate_transaction()
    
    # Use account_id as key for partitioning (same account goes to same partition)
    key = transaction['account_id']
    
    # Send to Kafka
    future = producer.send(
        topic=transaction_topic,
        key=key,
        value=transaction
    )
    
    # Get result (optional)
    try:
        record_metadata = future.get(timeout=10)
        print(f"✅ Sent transaction {transaction['transaction_id']}:")
        print(f"   Account: {transaction['account_id']}")
        print(f"   Amount: ${transaction['amount']}")
        print(f"   Category: {transaction['category']}")
        print(f"   Partition: {record_metadata.partition}")
        if transaction['is_fraudulent']:
            print(f"   ⚠️  FLAGGED AS POTENTIAL FRAUD")
    except Exception as e:
        print(f"❌ Error sending transaction: {e}")

print("🚀 Starting Kafka Transaction Producer")
print("Press Ctrl+C to stop\n")

try:
    counter = 0
    while True:
        send_transaction()
        counter += 1
        
        # Send every 1-3 seconds (simulating real-time stream)
        delay = random.uniform(1, 3)
        time.sleep(delay)
        
        # Every 10 transactions, show summary
        if counter % 10 == 0:
            print(f"\n📊 Produced {counter} transactions so far...\n")
            
except KeyboardInterrupt:
    print(f"\n\n🛑 Stopped. Total transactions produced: {counter}")
finally:
    producer.flush()
    producer.close()
    print("✅ Producer closed")
