## Start Zookeeper
## Open new Windows PowerShell
cd C:\kafka_2.12-3.5.0
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

## Start Kafka Server
## Open Second Windows PowerShell
cd C:\kafka_2.12-3.5.0
.\bin\windows\kafka-server-start.bat .\config\server.properties

## Initiate Kafka Cluster (Second Kafka Server)
## Open Third Windows PowerShell
cd C:\kafka_2.12-3.5.0-Copy
.\bin\windows\kafka-server-start.bat .\config\server.properties

## Create Kafka Topic
## Open Terminal 4
cd C:\kafka_2.12-3.5.0

## List existing topics
.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092

## Delete specific topic if it exists
.\bin\windows\kafka-topics.bat --delete --topic news-api-topic-test --bootstrap-server localhost:9092

## Describe Topic
.\bin\windows\kafka-topics.bat --describe --topic news-api-topic --bootstrap-server localhost:9092

## Create topics (both for your pipeline)
.\bin\windows\kafka-topics.bat --create --topic news-api-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

## Test Database Connection
## Terminal 5
cd C:\kafka_2.12-3.5.0\kafka-news-pipeline\kafka-producer
sbt "runMain DatabaseTest"

## Start Applications
## Terminal 6 - Consumer 1
cd C:\kafka_2.12-3.5.0\kafka-news-pipeline
sbt "runMain NewsConsumerDB1"

## Terminal 7 - Consumer 2
cd C:\kafka_2.12-3.5.0\kafka-news-pipeline
sbt "runMain NewsConsumerDB2"

## Terminal 8 - Producer
cd C:\kafka_2.12-3.5.0\kafka-news-pipeline
sbt "runMain producer.NewsProducer"


##=============================================================
## 📊 MONITORING COMMANDS (Terminal 9 - Monitoring Dashboard)
##=============================================================

echo "🔍 KAFKA NEWS PIPELINE MONITORING DASHBOARD"
echo "============================================="

## 1. KAFKA CLUSTER HEALTH
echo "📋 1. Kafka Cluster Health:"
.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
.\bin\windows\kafka-topics.bat --describe --bootstrap-server localhost:9092

## 2. CONSUMER GROUPS MONITORING
echo "📋 2. Consumer Groups Status:"
.\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --list
.\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --group news-db-consumer-group --describe

## 3. TOPIC DETAILS
echo "📋 3. Topic Details:"
.\bin\windows\kafka-topics.bat --describe --topic news-api-data --bootstrap-server localhost:9092
.\bin\windows\kafka-topics.bat --describe --topic live-api-data --bootstrap-server localhost:9092

## 4. REAL-TIME MESSAGE MONITORING (Optional - for testing)
echo "📋 4. Real-time Message Flow (last 5 messages):"
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic news-api-data --from-beginning --max-messages 5

## 5. BROKER INFORMATION
echo "📋 5. Broker Information:"
.\bin\windows\kafka-broker-api-versions.bat --bootstrap-server localhost:9092

##=============================================================
## 📊 DATABASE MONITORING QUERIES
##=============================================================

echo "📊 DATABASE MONITORING QUERIES:"
echo "================================"
echo "-- 1. Pipeline Health Dashboard"
echo "SELECT COUNT(*) as total_articles, MAX(created_at) as latest_article, MIN(created_at) as oldest_article, COUNT(DISTINCT category) as categories, COUNT(DISTINCT source) as sources FROM news_articles;"
echo ""
echo "-- 2. Real-time Activity (last hour)"
echo "SELECT category, COUNT(*) as articles_last_hour FROM news_articles WHERE created_at > NOW() - INTERVAL '1 hour' GROUP BY category ORDER BY articles_last_hour DESC;"
echo ""
echo "-- 3. Consumer Performance"
echo "SELECT DATE_TRUNC('minute', created_at) as minute, COUNT(*) as articles_per_minute FROM news_articles WHERE created_at > NOW() - INTERVAL '1 hour' GROUP BY minute ORDER BY minute DESC LIMIT 10;"
echo ""
echo "-- 4. Source Distribution"
echo "SELECT source, COUNT(*) as article_count, MAX(created_at) as latest_from_source FROM news_articles GROUP BY source ORDER BY article_count DESC;"
echo ""

## Clean All Topics at Once
echo "🗑️ CLEAN ALL TOPICS:"
echo "===================="

# Get list of all topics and delete them
FOR /F "tokens=*" %i IN ('.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092') DO .\bin\windows\kafka-topics.bat --delete --topic %i --bootstrap-server localhost:9092

# OR PowerShell version (more reliable):
echo "# PowerShell command to delete all topics:"
echo ".\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092 | ForEach-Object { .\bin\windows\kafka-topics.bat --delete --topic \$_ --bootstrap-server localhost:9092 }"

##=============================================================
## 🔧 TROUBLESHOOTING COMMANDS
##=============================================================

echo "🔧 TROUBLESHOOTING COMMANDS:"
echo "============================"
echo "# Check port usage:"
echo "netstat -ano | findstr :9092"
echo "netstat -ano | findstr :2181"
echo "netstat -ano | findstr :5432"
echo ""
echo "# Kill Java processes if needed:"
echo "Get-Process | Where-Object {\$_.ProcessName -eq 'java'} | Stop-Process -Force"
echo ""
echo "# Clean Kafka logs (when stopped):"
echo "Remove-Item -Recurse -Force C:\kafka_2.12-3.5.0\kafka-logs"
echo "Remove-Item -Recurse -Force C:\kafka_2.12-3.5.0\logs"
echo ""
echo "# Reset consumer group offsets:"
echo ".\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --group news-db-consumer-group --reset-offsets --to-earliest --topic news-api-data --execute"