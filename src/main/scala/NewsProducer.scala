package producer

import java.util.Properties
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer
import com.typesafe.config.ConfigFactory
import models.NewsArticle
import scala.io.Source
import java.net.HttpURLConnection
import java.net.URL
import java.sql.Timestamp

object NewsProducer {
  val config = ConfigFactory.load()
  val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  // Kafka config
  val bootstrapServers = config.getString("kafka.bootstrap-servers")
  val topic = config.getString("kafka.topic")

  // NewsAPI config
  val apiKey = config.getString("newsapi.api-key")
  val baseUrl = config.getString("newsapi.base-url")
  val categories = config.getStringList("newsapi.categories").asScala
  val articlesPerCategory = config.getInt("newsapi.articles-per-category")
  val delayMs = config.getInt("rate-limiting.article-delay-ms")
  val apiDelayMs = config.getInt("rate-limiting.api-call-delay-ms")

  // Kafka Producer config
  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  val producer = new KafkaProducer[String, String](props)

  def fetchNews(category: String): Seq[NewsArticle] = {
    val urlStr =
      s"$baseUrl/top-headlines?country=us&category=$category&pageSize=$articlesPerCategory&apiKey=$apiKey"

    val conn = new URL(urlStr).openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("GET")

    Try {
      val response = Source.fromInputStream(conn.getInputStream).mkString
      val json = mapper.readTree(response)
      val articles = json.get("articles")

      articles.elements().asScala.toSeq.map { node =>
        NewsArticle(
          id = None,
          title = node.get("title").asText(),
          description = Option(node.get("description")).map(_.asText()),
          url = node.get("url").asText(),
          author = Option(node.get("author")).map(_.asText()),
          publishedAt = node.get("publishedAt").asText(),
          content = Option(node.get("content")).map(_.asText()),
          source = node.get("source").get("name").asText(),
          category = category,
          country = "us",
          language = "en",
          timestamp = System.currentTimeMillis(),
          createdAt = Some(new Timestamp(System.currentTimeMillis()))
        )
      }
    } match {
      case Success(articles) => articles
      case Failure(e) =>
        println(s"❌ Failed to fetch news for $category: ${e.getMessage}")
        Seq.empty
    }
  }

  def main(args: Array[String]): Unit = {
    println("🧩 Config keys loaded: " + config.entrySet().asScala.map(_.getKey).mkString(", "))
    println(s"🚀 Starting NewsProducer for topic '$topic'")

    while (true) {
      categories.foreach { category =>
        println(s"🔍 Fetching category: $category")
        val articles = fetchNews(category)

        articles.foreach { article =>
          val json = mapper.writeValueAsString(article)
          val record = new ProducerRecord[String, String](topic, article.title, json)
          producer.send(record)
          println(s"📤 Sent: ${article.title}")
          Thread.sleep(delayMs)
        }

        Thread.sleep(apiDelayMs)
      }
    }
    // producer.close() // Not reached due to infinite loop, but add if you ever break the loop
  }
}