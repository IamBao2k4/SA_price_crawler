# Setup Guide

## Prerequisites

- Python 3.11+
- Docker & Docker Compose (for remote mode)
- Access to Kafka and MongoDB servers

## Configuration

### 1. Create `.env` file

Copy from example:
```bash
cp .env.example .env
```

### 2. Edit `.env` with your settings

```bash
# For Remote Server (e.g., AWS)
KAFKA_BOOTSTRAP_SERVERS=YOUR_SERVER_IP:9092
MONGODB_URI=mongodb://YOUR_SERVER_IP:27020/

# For Local Development
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
MONGODB_URI=mongodb://localhost:27017/
```

**Important:** Never commit `.env` file to git!

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

## Running

### Option 1: Remote Server Mode

```bash
# Test connection first
python test_connection.py

# Run with Docker
docker-compose -f docker-compose.remote.yml up -d

# Or run locally
python main_producer.py
python main_consumer.py
```

### Option 2: Local Development

```bash
# Start local Kafka & MongoDB
docker-compose up -d kafka mongodb

# Run services
python main_producer.py
python main_consumer.py
```

## Security Notes

- ⚠️ `.env` file is ignored by git (contains sensitive data)
- ⚠️ Never hardcode server IPs or credentials in code
- ⚠️ Use `.env.example` as template only
- ✅ Keep production credentials separate from development

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | ✅ Yes | - | Kafka server address |
| `MONGODB_URI` | ✅ Yes | - | MongoDB connection string |
| `KAFKA_TOPIC_PREFIX` | No | `binance.klines` | Kafka topic prefix |
| `KAFKA_GROUP_ID` | No | `binance-consumer-group` | Consumer group ID |
| `MONGODB_DATABASE` | No | `binance` | MongoDB database name |
| `ACTIVE_INTERVALS` | No | `1m,5m,15m,1h,4h,1d` | Intervals to crawl |
| `LOG_LEVEL` | No | `INFO` | Logging level |
| `TZ` | No | `Asia/Ho_Chi_Minh` | Timezone |

## Verification

```bash
# Test connections
python test_connection.py

# Check logs
docker logs binance_crawler
docker logs binance_consumer
```
