from kafka import KafkaProducer
import psycopg2
import json
import time
import random
from datetime import datetime

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# PostgreSQL connection (PORT 5433)
def get_postgres_connection():
    return psycopg2.connect(
        host="localhost",
        port=5433,  # CHANGED FROM 5432 TO 5433
        database="postgres",
        user="postgres",
        password="mysecretpassword"
    )

def get_current_balances():
    """Get current account balances from PostgreSQL"""
    conn = get_postgres_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT account, SUM(amount) as balance
        FROM transactions
        GROUP BY account
        ORDER BY account
    """)
    
    balances = {row[0]: float(row[1] or 0) for row in cursor.fetchall()}
    
    cursor.close()
    conn.close()
    
    return balances

def simulate_balance_change():
    """Simulate a balance change (in real project, this would detect actual changes)"""
    conn = get_postgres_connection()
    cursor = conn.cursor()
    
    # Get a random account
    cursor.execute("SELECT DISTINCT account FROM transactions LIMIT 5")
    accounts = [row[0] for row in cursor.fetchall()]
    
    if not accounts:
        cursor.close()
        conn.close()
        return None
    
    account = accounts[0]
    
    # Add a new transaction to simulate change
    cursor.execute("""
        INSERT INTO transactions (date, amount, category, account)
        VALUES (CURRENT_DATE, %s, %s, %s)
        RETURNING id, date, amount
    """, (100.00, 'CDC_Simulation', account))
    
    new_transaction = cursor.fetchone()
    
    conn.commit()
    
    # Get updated balance
    cursor.execute("SELECT SUM(amount) FROM transactions WHERE account = %s", (account,))
    new_balance = float(cursor.fetchone()[0] or 0)
    
    cursor.close()
    conn.close()
    
    return {
        'account': account,
        'transaction_id': new_transaction[0],
        'transaction_date': new_transaction[1].isoformat(),
        'transaction_amount': float(new_transaction[2]),
        'new_balance': new_balance,
        'change_type': 'INSERT',
        'change_timestamp': datetime.now().isoformat()
    }

def send_balance_change(change_event):
    """Send balance change event to Kafka"""
    producer.send('account_balances', value=change_event)
    producer.flush()
    
    print(f"📤 CDC Event Sent:")
    print(f"   Account: {change_event['account']}")
    print(f"   Change: ${change_event['transaction_amount']:.2f}")
    print(f"   New Balance: ${change_event['new_balance']:.2f}")
    print(f"   Type: {change_event['change_type']}")

def create_cdc_consumer():
    """Create a consumer to listen for CDC events"""
    from kafka import KafkaConsumer
    
    consumer = KafkaConsumer(
        'account_balances',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='cdc-consumer-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print("\n👂 CDC Consumer listening for balance changes...")
    return consumer

# Main execution
print("🔍 Change Data Capture (CDC) System")
print("=" * 50)

# 1. Show initial balances
print("\n1. Current Account Balances:")
try:
    initial_balances = get_current_balances()
    for account, balance in initial_balances.items():
        print(f"   {account}: ${balance:.2f}")
except Exception as e:
    print(f"   ⚠️  Could not fetch balances: {e}")
    print("   Starting with empty balances...")
    initial_balances = {}

# 2. Start CDC consumer in background thread
import threading

def listen_for_changes():
    consumer = create_cdc_consumer()
    event_count = 0
    
    for message in consumer:
        event = message.value
        event_count += 1
        
        print(f"\n📥 CDC Event Received #{event_count}:")
        print(f"   Account: {event['account']}")
        print(f"   New Balance: ${event['new_balance']:.2f}")
        print(f"   Change: ${event['transaction_amount']:.2f}")
        print(f"   Time: {event['change_timestamp']}")

# Start consumer thread
consumer_thread = threading.Thread(target=listen_for_changes, daemon=True)
consumer_thread.start()

time.sleep(2)  # Let consumer start

# 3. Simulate balance changes
print("\n2. Simulating Balance Changes (CDC Events):")
print("   Press Ctrl+C to stop\n")

try:
    change_count = 0
    while True:
        # Simulate a balance change every 10-20 seconds
        time.sleep(random.uniform(10, 20))
        
        change_event = simulate_balance_change()
        if change_event:
            send_balance_change(change_event)
            change_count += 1
            
except KeyboardInterrupt:
    print(f"\n\n🛑 CDC System Stopped")
    print(f"   Total changes simulated: {change_count}")

finally:
    producer.close()
    print("✅ CDC Producer closed")
