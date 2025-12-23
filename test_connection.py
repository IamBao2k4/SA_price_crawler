"""
Test connection to remote services
"""
import sys
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient
from pymongo import MongoClient
from src.config.settings import Settings

def test_kafka():
    """Test Kafka connection"""
    print("=" * 70)
    print("Testing Kafka Connection...")
    print("=" * 70)

    settings = Settings()
    print(f"Kafka Bootstrap: {settings.kafka.bootstrap_servers}")

    try:
        producer = KafkaProducer(
            bootstrap_servers=settings.kafka.bootstrap_servers.split(','),
            request_timeout_ms=10000
        )
        print("✅ Kafka Producer: Connected")

        # List topics using AdminClient
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=settings.kafka.bootstrap_servers.split(','),
                request_timeout_ms=10000
            )
            topics = admin.list_topics()
            print(f"✅ Available topics: {len(topics)} topics")
            binance_topics = [t for t in topics if 'binance' in t]
            if binance_topics:
                print(f"   Binance topics: {binance_topics}")
            admin.close()
        except Exception as e:
            print(f"⚠️  Could not list topics: {e}")

        producer.close()
        return True
    except Exception as e:
        print(f"❌ Kafka Error: {e}")
        return False

def test_mongodb():
    """Test MongoDB connection"""
    print("\n" + "=" * 70)
    print("Testing MongoDB Connection...")
    print("=" * 70)

    settings = Settings()
    print(f"MongoDB URI: {settings.mongodb.uri}")
    print(f"Database: {settings.mongodb.database}")

    try:
        client = MongoClient(settings.mongodb.uri, serverSelectionTimeoutMS=5000)

        # Test connection with ping (doesn't require auth)
        client.admin.command('ping')
        print("✅ MongoDB: Connected")

        # Try to access the specific database (may work without listDatabases permission)
        db = client[settings.mongodb.database]
        try:
            collections = db.list_collection_names()
            print(f"✅ Collections in '{settings.mongodb.database}': {collections}")

            # Count documents
            if 'klines' in collections:
                count = db.klines.count_documents({})
                print(f"✅ Documents in klines: {count:,}")
        except Exception as e:
            print(f"⚠️  Could not list collections (may need auth): {e}")

        client.close()
        return True
    except Exception as e:
        print(f"❌ MongoDB Error: {e}")
        return False

def main():
    """Run all tests"""
    print("\n🔍 Testing Remote Services Connection")
    print("Server: 3.26.179.20\n")

    kafka_ok = test_kafka()
    mongo_ok = test_mongodb()

    print("\n" + "=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"Kafka:   {'✅ OK' if kafka_ok else '❌ FAILED'}")
    print(f"MongoDB: {'✅ OK' if mongo_ok else '❌ FAILED'}")
    print("=" * 70)

    if kafka_ok and mongo_ok:
        print("\n✅ All services are reachable!")
        print("\nYou can now run:")
        print("  python main_producer.py")
        print("  python main_consumer.py")
        return 0
    else:
        print("\n❌ Some services are not reachable.")
        print("\nPlease check:")
        print("  1. Server IP is correct (3.26.179.20)")
        print("  2. Firewall/Security groups allow your IP")
        print("  3. Services are running on the server")
        return 1

if __name__ == "__main__":
    sys.exit(main())
