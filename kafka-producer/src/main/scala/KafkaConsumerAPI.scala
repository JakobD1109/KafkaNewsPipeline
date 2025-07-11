import java.util.Properties
import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerRecords, ConsumerRecord}
import scala.collection.JavaConverters._
import scala.util.{Try, Success, Failure}
import java.sql.Timestamp
import java.time.Instant
import slick.jdbc.PostgresProfile.api._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

// Case class representing a news article
case class NewsArticle(
  id: Option[Int] = None,
  title: String,
  description: Option[String],
  author: Option[String],
  url: String,
  publishedAt: String,
  source: String,
  category: String,
  timestamp: Long,
  createdAt: Option[Timestamp] = Some(Timestamp.from(Instant.now()))
)

// Table definition
class NewsArticles(tag: Tag) extends Table[NewsArticle](tag, "news_articles") {
  def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
  def title = column[String]("title")
  def description = column[Option[String]]("description")
  def author = column[Option[String]]("author")
  def url = column[String]("url")
  def publishedAt = column[String]("published_at")
  def source = column[String]("source")
  def category = column[String]("category")
  def timestamp = column[Long]("timestamp")
  def createdAt = column[Option[Timestamp]]("created_at")
  
  def * = (id.?, title, description, author, url, publishedAt, source, category, timestamp, createdAt) <> (NewsArticle.tupled, NewsArticle.unapply)
}

object NewsConsumerDB {

  // ExecutionContext inside the object
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global

  // Jackson mapper for JSON parsing
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  // Load configuration
  val config = ConfigFactory.load()
  
  // Database configuration using your application.conf
  val db = Database.forConfig("postgres", config)
  val articles = TableQuery[NewsArticles]

  // Kafka configuration from your application.conf
  val kafkaBootstrapServers = config.getString("kafka.bootstrap-servers")
  val kafkaTopic = config.getString("kafka.topic")
  val kafkaConsumerGroup = config.getString("kafka.consumer-group")

  def saveArticle(article: NewsArticle): Future[Int] = {
    // Fixed database operation
    val existing = articles.filter(_.url === article.url)
    
    db.run(existing.result.headOption).flatMap {
      case Some(_) => 
        println(s"📄 Article already exists: ${article.title}")
        Future.successful(0)
      case None => 
        val insertQuery = articles += article.copy(id = None)
        db.run(insertQuery).map { _ =>
          println(s"💾 Saved to DB: ${article.title}")
          1
        }
    }
  }

  def processRecord(record: ConsumerRecord[String, String]): Unit = {
    Try {
      // Use Jackson instead of Circe
      val jsonData = mapper.readValue(record.value(), classOf[Map[String, Any]])
      
      val article = NewsArticle(
        id = None,
        title = jsonData.getOrElse("title", "").toString,
        description = Option(jsonData.get("description")).flatMap(Option(_)).map(_.toString).filter(_.nonEmpty),
        author = Option(jsonData.get("author")).flatMap(Option(_)).map(_.toString).filter(_.nonEmpty),
        url = jsonData.getOrElse("url", "").toString,
        publishedAt = jsonData.getOrElse("publishedAt", "").toString,
        source = jsonData.getOrElse("source", "").toString,
        category = jsonData.getOrElse("category", "").toString,
        timestamp = jsonData.getOrElse("timestamp", Instant.now.toEpochMilli) match {
          case l: Long => l
          case s: String => Try(s.toLong).getOrElse(Instant.now.toEpochMilli)
          case i: Int => i.toLong
          case _ => Instant.now.toEpochMilli
        }
      )
      
      val future = saveArticle(article)
      Try(Await.result(future, 10.seconds)) match {
        case Success(_) => // Successfully saved
        case Failure(ex) => println(s"❌ Database save error: ${ex.getMessage}")
      }
    } match {
      case Success(_) => // Success
      case Failure(exception) => 
        println(s"❌ Error processing record: ${exception.getMessage}")
    }
  }

  def startConsumer(): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", kafkaBootstrapServers)
    props.put("group.id", kafkaConsumerGroup)
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "earliest")
    props.put("enable.auto.commit", "true")

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(java.util.Arrays.asList(kafkaTopic))

    println("🔄 Starting Scala Kafka consumer with PostgreSQL integration...")
    println(s"📡 Listening for news articles on topic: $kafkaTopic")
    println(s"🏷️  Consumer group: $kafkaConsumerGroup")
    println(s"🗄️  Database: ${config.getString("postgres.url")}")

    // Display current stats
    showStats()

    try {
      while (true) {
        val records: ConsumerRecords[String, String] = consumer.poll(java.time.Duration.ofMillis(1000))
        
        for (record <- records.asScala) {
          println(s"📨 Received from partition ${record.partition()}: ${record.value().take(100)}...")
          processRecord(record)
        }
        
        if (!records.isEmpty) {
          showStats()
        }
      }
    } catch {
      case e: Exception =>
        println(s"❌ Consumer error: ${e.getMessage}")
    } finally {
      consumer.close()
      db.close()
    }
  }

  // Fixed showStats method
  def showStats(): Unit = {
    Try {
      val totalFuture = db.run(articles.length.result)
      val total = Await.result(totalFuture, 5.seconds)
      println(s"📊 Total articles in database: $total")

      val categoryFuture = db.run(
        articles.groupBy(_.category)
          .map { case (category, group) => (category, group.length) }
          .result
      )
      val categories = Await.result(categoryFuture, 5.seconds)
      
      if (categories.nonEmpty) {
        println("📈 Articles by category:")
        categories.foreach { case (category, count) =>
          println(s"   $category: $count")
        }
      }
    } match {
      case Success(_) => // Success - stats displayed
      case Failure(e) => println(s"❌ Error getting stats: ${e.getMessage}")
    }
  }

  def main(args: Array[String]): Unit = {
    println("🚀 Starting News Consumer...")
    println(s"📋 Configuration loaded:")
    println(s"   Database: ${config.getString("postgres.url")}")
    println(s"   Kafka: ${config.getString("kafka.bootstrap-servers")}")
    println(s"   Topic: ${config.getString("kafka.topic")}")
    
    startConsumer()
  }
}