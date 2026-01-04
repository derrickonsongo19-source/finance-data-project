from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# Kafka configuration
bootstrap_servers = 'localhost:9092'

# Topics to create
topics = [
    NewTopic(name="financial_transactions", num_partitions=3, replication_factor=1),
    NewTopic(name="account_balances", num_partitions=2, replication_factor=1),
    NewTopic(name="fraud_alerts", num_partitions=1, replication_factor=1),
]

try:
    # Create admin client
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    
    # Create topics
    admin_client.create_topics(new_topics=topics, validate_only=False)
    print("✅ Kafka topics created successfully:")
    for topic in topics:
        print(f"   - {topic.name} ({topic.num_partitions} partitions)")
    
    # List all topics
    existing_topics = admin_client.list_topics()
    print(f"\n📋 Total topics: {len(existing_topics)}")
    
except TopicAlreadyExistsError:
    print("⚠️  Topics already exist")
except Exception as e:
    print(f"❌ Error: {e}")
finally:
    if 'admin_client' in locals():
        admin_client.close()
