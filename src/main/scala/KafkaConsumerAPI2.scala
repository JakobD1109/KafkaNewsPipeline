import java.time.Duration
import java.util.Properties
import scala.jdk.CollectionConverters._
import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerConfig}
import org.apache.kafka.common.serialization.StringDeserializer
import com.typesafe.config.ConfigFactory
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import slick.jdbc.PostgresProfile.api._
import models.NewsArticle
import java.sql.Timestamp
import scala.concurrent.Await
import scala.concurrent.duration._

object NewsConsumerDB2 {
  val config = ConfigFactory.load()
  val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  // Kafka config
  val kafkaBootstrapServers = config.getString("kafka.bootstrap-servers")
  val kafkaTopic = config.getString("kafka.topic")
  val consumerGroup = config.getString("kafka.consumer-group")

  // DB config
  val dbUrl = config.getString("postgres.url")
  val dbUser = config.getString("postgres.user")
  val dbPassword = config.getString("postgres.password")
  val db = Database.forURL(
    url = dbUrl,
    user = dbUser,
    password = dbPassword,
    driver = "org.postgresql.Driver"
  )

  // ✅ Slick table mapping 
  class NewsTable(tag: Tag) extends Table[NewsArticle](tag, "news_articles") {
    def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
    def title = column[String]("title")
    def description = column[Option[String]]("description")
    def url = column[String]("url")
    def author = column[Option[String]]("author")
    def publishedAt = column[String]("published_at")
    def content = column[Option[String]]("content")
    def source = column[String]("source")
    def category = column[String]("category")
    def country = column[String]("country")
    def language = column[String]("language")
    def timestamp = column[Long]("timestamp")
    def createdAt = column[Option[Timestamp]]("created_at")

    def * = (
      id.?, title, description, url, author, publishedAt, content,
      source, category, country, language, timestamp, createdAt
    ) <> (NewsArticle.tupled, NewsArticle.unapply)
  }

  val newsArticles = TableQuery[NewsTable]

  def insertArticle(article: NewsArticle): Unit = {
    val insertAction = newsArticles += article
    Await.result(db.run(insertAction), 5.seconds)
    println(s" Inserted: ${article.title}")
  }

  def startConsumer(): Unit = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(List(kafkaTopic).asJava)

    println(s" NewsConsumerDB1 subscribed to topic '$kafkaTopic'")

    while (true) {
      val records = consumer.poll(java.time.Duration.ofMillis(1000)).asScala

      if (records.nonEmpty) {
        println(s" Received ${records.size} record(s)")
      }

      records.foreach { record =>
        try {
          println(s" Raw JSON: ${record.value()}")
          val article = mapper.readValue(record.value(), classOf[NewsArticle])
          println(s" Parsed article: $article")
          insertArticle(article)
        } catch {
          case e: Exception =>
            println(s" Failed to process record: ${e.getMessage}")
            e.printStackTrace()
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    startConsumer()
  }
}
