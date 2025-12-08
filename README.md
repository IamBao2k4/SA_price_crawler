# Binance Crawler - Clean Architecture

Real-time cryptocurrency data pipeline using Kafka + MongoDB with Clean Architecture.

## 🏗️ Architecture

```
Binance API → Producer → Kafka → Consumer → MongoDB
              (Crawler)         (Storage)
```

**Tech Stack:**
- **Data Source:** Binance API
- **Message Queue:** Apache Kafka
- **Database:** MongoDB
- **Language:** Python 3.11+
- **Architecture:** Clean Architecture (SOLID principles)

## 🚀 Quick Start

### Option 1: Docker (Recommended)

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f crawler consumer

# Monitor via Kafka UI
open http://localhost:8080
```

### Option 2: Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Start Kafka & MongoDB (Docker)
docker-compose up -d kafka mongodb

# Run producer
python main_producer.py

# Run consumer (in another terminal)
python main_consumer.py
```

## 📁 Project Structure

```
binance_crawler/
├── src/                    # Source code (Clean Architecture)
│   ├── config/            # Configuration management
│   ├── core/              # Domain models & interfaces
│   ├── producers/         # Data sources (Binance)
│   ├── storage/           # Infrastructure (Kafka, MongoDB)
│   ├── consumers/         # Application services
│   └── utils/             # Utilities
│
├── main_producer.py       # Entry point: Producer
├── main_consumer.py       # Entry point: Consumer
│
├── docker-compose.yml     # Docker orchestration
├── Dockerfile             # Container image
├── requirements.txt       # Python dependencies
├── symbols_top20.txt      # Trading symbols list
│
├── ARCHITECTURE.md        # Architecture documentation
├── DOCKER_GUIDE.md        # Docker detailed guide
├── DOCKER_QUICKSTART.md   # Docker quick start
├── REFACTORING.md         # Migration from old code
│
└── legacy/                # Old code (deprecated)
```

## ⚙️ Configuration

Configure via environment variables:

```bash
# Kafka
export KAFKA_BOOTSTRAP_SERVERS=kafka:9092
export KAFKA_TOPIC_PREFIX=binance.klines

# MongoDB
export MONGODB_URI=mongodb://admin:admin123@mongodb:27017/
export MONGODB_DATABASE=binance

# Crawler
export ACTIVE_INTERVALS=1m,5m,15m,1h,4h,1d
export LOG_LEVEL=INFO
```

Or use `.env` file (see `.env.example`).

## 📊 Features

### Producer (Crawler)
✅ Crawl multiple intervals (1m, 5m, 15m, 1h, 4h, 1d, etc.)
✅ Real-time data streaming to Kafka
✅ Smart update frequency (no missing candles)
✅ Automatic retry on errors
✅ Rate limit protection

### Consumer (Storage)
✅ Batch processing for efficiency
✅ Upsert to MongoDB (no duplicates)
✅ Automatic indexing
✅ Statistics tracking

### Infrastructure
✅ Kafka for reliable messaging
✅ MongoDB for persistent storage
✅ Docker for easy deployment
✅ Kafka UI for monitoring

## 🎯 Use Cases

- **Real-time Trading Bots:** Get latest candle data < 15s latency
- **Backtesting:** Query historical data from MongoDB
- **Analytics:** Process data from Kafka stream
- **Multiple Consumers:** Add more consumers for different purposes

## 📚 Documentation

- **[ARCHITECTURE.md](ARCHITECTURE.md)** - Clean Architecture details
- **[DOCKER_QUICKSTART.md](DOCKER_QUICKSTART.md)** - Docker quick start
- **[DOCKER_GUIDE.md](DOCKER_GUIDE.md)** - Docker detailed guide
- **[REFACTORING.md](REFACTORING.md)** - Migration from old code

## 🧪 Testing

```bash
# Test producer
python main_producer.py

# Test consumer
python main_consumer.py

# Check MongoDB
docker exec -it binance_mongodb mongosh -u admin -p admin123
> use binance
> db.klines.countDocuments()

# Check Kafka UI
open http://localhost:8080
```

## 📈 Monitoring

### Kafka UI
- URL: http://localhost:8080
- View topics, messages, consumer lag

### MongoDB
```bash
# Connect
docker exec -it binance_mongodb mongosh -u admin -p admin123

# Query
use binance
db.klines.countDocuments()
db.klines.find({symbol: "BTCUSDT", interval: "1h"}).sort({open_time: -1}).limit(5)
```

### Logs
```bash
# Docker logs
docker-compose logs -f crawler
docker-compose logs -f consumer

# Local logs
./logs/
```

## 🔧 Development

### Add New Data Source

Implement `IDataSource` interface:

```python
# src/producers/coinbase.py
from src.core.interfaces import IDataSource

class CoinbaseDataSource(IDataSource):
    def fetch_klines(self, symbol, interval, limit):
        # Implement Coinbase API
        pass
```

### Add New Storage

Implement `IStorage` interface:

```python
# src/storage/postgres_storage.py
from src.core.interfaces import IStorage

class PostgresStorage(IStorage):
    def save_batch(self, klines):
        # Implement Postgres logic
        pass
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for more details.

## 🐛 Troubleshooting

### Producer not publishing?
```bash
# Check Kafka is running
docker-compose ps kafka

# Check logs
docker-compose logs crawler
```

### Consumer not consuming?
```bash
# Check consumer group
docker exec binance_kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group binance-consumer-group
```

### MongoDB connection error?
```bash
# Check MongoDB
docker-compose ps mongodb
docker-compose logs mongodb
```

See [DOCKER_GUIDE.md](DOCKER_GUIDE.md) for more troubleshooting.

## 📦 Dependencies

```
requests>=2.31.0        # HTTP client
pandas>=2.2.0           # Data processing
kafka-python>=2.0.2     # Kafka client
pymongo>=4.6.0          # MongoDB client
schedule>=1.2.0         # Task scheduling
```

## 🎓 Learn More

- **Clean Architecture:** See [ARCHITECTURE.md](ARCHITECTURE.md)
- **Kafka Basics:** See [DOCKER_GUIDE.md](DOCKER_GUIDE.md)
- **MongoDB Queries:** MongoDB documentation

## 📝 License

MIT License - Feel free to use for your projects.

## 🤝 Contributing

1. Fork the repo
2. Create feature branch
3. Follow Clean Architecture principles
4. Submit PR

## 📞 Support

- Issues: GitHub Issues
- Questions: GitHub Discussions

---

**Built with Clean Architecture 🏛️ | Powered by Kafka + MongoDB ⚡**
