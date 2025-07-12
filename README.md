# Kafka News Pipeline

Real-time news data pipeline using Kafka, Scala, and PostgreSQL.

## 🚀 Features

- Fetches news from NewsAPI every 2 minutes
- Processes 4 categories: business, technology, health, science
- Stores articles in PostgreSQL with duplicate detection
- Real-time Kafka streaming 
- Configurable rate limiting and error handling

## 📋 Prerequisites

- Java 11+
- Scala 2.12
- Apache Kafka 2.12-3.5.0
- PostgreSQL 12+
- SBT (Scala Build Tool)
- NewsAPI key (free tier available)

## 🛠️ Quick Setup

### 1. Clone Repository
```bash
git clone https://github.com/yourusername/kafka-news-pipeline.git
cd kafka-news-pipeline
```

### 2. Setup Environment
```bash
cp .env.example .env
# Edit .env with your actual credentials
```

### 3. Setup Database
```bash
# Create database
createdb your_database_name

# Run schema
psql -d your_database_name -f setup/database/schema.sql
```

### 4. Setup Kafka
```bash
# Start Kafka (Windows)
cd C:\kafka_2.12-3.5.0
.\bin\windows\kafka-server-start.bat .\config\server.properties

# Create topic
.\bin\windows\kafka-topics.bat --create --topic news-api-data --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### 5. Run Application
```bash
cd kafka-producer

# Terminal 1 - Start Consumer
sbt "runMain NewsConsumerDB"

# Terminal 2 - Start Producer  
sbt "runMain NewsProducerApp"
```

## 📊 Monitoring

Check consumer lag:
```bash
.\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --group news-consumer-group --describe
```

Check database:
```sql
SELECT COUNT(*) FROM news_articles;
SELECT category, COUNT(*) FROM news_articles GROUP BY category;
```

## 🔧 Configuration

All settings in `.env`:
- `FETCH_INTERVAL_MINUTES`: How often to fetch news (default: 2.0)
- `API_DELAY_MS`: Delay between API categories (default: 3000)
- `ARTICLE_DELAY_MS`: Delay between articles (default: 1000)

## 📊 Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────────┐
│  Kafka Producer │───▶│   Kafka Topic    │───▶│   Consumer Group    │
│  (Avro Data)    │    │ demo_sub_topic_4 │    │ "load-balance-group"│
│                 │    │                  │    │                     │
│ CustomPartitioner│    │ ┌─────────────┐  │    │ ┌─────────────────┐ │
│      ↓          │    │ │ Partition 0 │  │    │ │   Consumer 1    │ │
│ key1→Partition 0│    │ │ key1, key3  │  │    │ │  (Partition 0)  │ │
│ key2→Partition 1│    │ └─────────────┘  │    │ └─────────────────┘ │
│ key3→Partition 0│    │ ┌─────────────┐  │    │ ┌─────────────────┐ │
│ key4→Partition 1│    │ │ Partition 1 │  │    │ │   Consumer 2    │ │
└─────────────────┘    │ │ key2, key4  │  │    │ │  (Partition 1)  │ │
                       │ └─────────────┘  │    │ └─────────────────┘ │
                       └──────────────────┘    └─────────────────────┘
                                                         │
                                               ┌─────────────────────┐
                                               │   Backup Consumer   │
                                               │ "backup-group"      │
                                               │ (Failover Support)  │
                                               └─────────────────────┘
```

