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
## Open Terminal
cd C:\kafka_2.12-3.5.0

## List existing topics
.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092

## Delete specific topic if it exists
.\bin\windows\kafka-topics.bat --delete --topic test-topic --bootstrap-server localhost:9092

## Create new topic
.\bin\windows\kafka-topics.bat --create --topic live-api-data --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1

## Test Database Connection
# Run this in Terminal 4 (Python)
cd C:\kafka_2.12-3.5.0\kafka-producer
sbt "runMain DatabaseTest"

## Run Kafka Producer and Consumer
## Open Individual Terminal
cd kafka-producer
sbt run

## 1 for Consumer
## 2 for Producer

#### Or

sbt "runMain NewsConsumerDB"

sbt "runMain NewsProducerApp"