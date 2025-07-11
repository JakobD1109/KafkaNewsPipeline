import java.util.{Properties, Timer, TimerTask}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.io.Source
import java.net.{HttpURLConnection, URL}
import java.time.Instant
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object Config {
  // Core settings
  val NEWSAPI_KEY: String = sys.env.getOrElse("NEWSAPI_KEY", 
    throw new RuntimeException("NEWSAPI_KEY not found in environment variables"))
  val KAFKA_BOOTSTRAP_SERVERS: String = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
  val KAFKA_TOPIC: String = sys.env.getOrElse("KAFKA_TOPIC", "news-api-data")
  
  // NewsAPI settings
  val CATEGORIES: List[String] = List("business", "technology", "health", "science")  // Reduced to avoid rate limits
  val COUNTRIES: List[String] = List("us", "gb", "ca")
  val FETCH_INTERVAL_MINUTES: Double = sys.env.getOrElse("FETCH_INTERVAL_MINUTES", "0.5").toDouble  // 5 minutes default
  val FETCH_INTERVAL_MS: Long = (FETCH_INTERVAL_MINUTES * 60 * 1000).toLong 
  val ARTICLES_PER_CATEGORY: Int = sys.env.getOrElse("ARTICLES_PER_CATEGORY", "5").toInt    // 5 articles per category
  
  // Rate limiting
  val ARTICLE_PROCESSING_DELAY_MS: Int = sys.env.getOrElse("ARTICLE_DELAY_MS", "1000").toInt
  val API_CALL_DELAY_MS: Int = sys.env.getOrElse("API_DELAY_MS", "3000").toInt  // 3 seconds between API calls
  
  // PostgreSQL (if needed)
  val DB_HOST: String = sys.env.getOrElse("DB_HOST", "localhost")
  val DB_PORT: String = sys.env.getOrElse("DB_PORT", "54806")
  val DB_NAME: String = sys.env.getOrElse("DB_NAME", "news_pipeline")
  val DB_USER: String = sys.env.getOrElse("DB_USER", "postgres")
  val DB_PASSWORD: String = sys.env.getOrElse("DB_PASSWORD", "12345678")
  val DATABASE_URL: String = s"postgresql://$DB_USER:$DB_PASSWORD@$DB_HOST:$DB_PORT/$DB_NAME"
  
  def displayConfig(): Unit = {
    println("🔧 Configuration:")
    println(s"├─ NewsAPI Key: ${if (NEWSAPI_KEY.nonEmpty) "✅ Set" else "❌ Missing"}")
    println(s"├─ Kafka: $KAFKA_BOOTSTRAP_SERVERS")
    println(s"├─ Topic: $KAFKA_TOPIC")
    println(s"├─ Categories: ${CATEGORIES.mkString(", ")}")
    println(s"├─ Fetch Interval: $FETCH_INTERVAL_MINUTES minutes")
    println(s"├─ Articles per Category: $ARTICLES_PER_CATEGORY")
    println(s"├─ Article Delay: ${ARTICLE_PROCESSING_DELAY_MS}ms")
    println(s"└─ API Call Delay: ${API_CALL_DELAY_MS}ms")
  }
}

class NewsFetcher(producer: KafkaProducer[String, String], topic: String) {

  val baseUrl = "https://newsapi.org/v2"
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def fetchTopHeadlines(category: String, pageSize: Int = 10): List[Map[String, Any]] = {
    val url = s"$baseUrl/top-headlines?apiKey=${Config.NEWSAPI_KEY}&country=us&category=$category&pageSize=$pageSize"
    try {
      val connection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
      connection.setRequestMethod("GET")
      val inputStream = connection.getInputStream
      val response = Source.fromInputStream(inputStream).mkString
      inputStream.close()

      val jsonData = mapper.readValue(response, classOf[Map[String, Any]])
      
      if (jsonData.getOrElse("status", "") == "ok") {
        val articles = jsonData.getOrElse("articles", List()).asInstanceOf[List[Map[String, Any]]]
        println(s"✅ Fetched ${articles.length} articles for category: $category")
        articles
      } else {
        println("❌ API returned error or unexpected format")
        List.empty
      }
    } catch {
      case e: Exception =>
        println(s"❌ Error fetching headlines: ${e.getMessage}")
        List.empty
    }
  }

  def transformArticle(article: Map[String, Any], category: String): String = {
    val transformed = Map(
      "title" -> article.getOrElse("title", "No Title"),
      "description" -> article.getOrElse("description", null),
      "author" -> article.getOrElse("author", null),
      "url" -> article.getOrElse("url", ""),
      "publishedAt" -> article.getOrElse("publishedAt", ""),
      "source" -> article.get("source").collect { 
        case m: Map[String, Any] @unchecked => m.getOrElse("name", "Unknown Source") 
      }.getOrElse("Unknown Source"),
      "category" -> category,
      "timestamp" -> Instant.now.toEpochMilli
    )
    
    mapper.writeValueAsString(transformed)
  }

  def sendToKafka(articleJson: String, category: String, timestamp: Long): Unit = {
    val key = s"news_${timestamp}"
    val record = new ProducerRecord[String, String](topic, key, articleJson)

    try {
      val metadata = producer.send(record).get()
      println(s"📨 Sent to Kafka - Topic: ${metadata.topic()}, Partition: ${metadata.partition()}, Offset: ${metadata.offset()}")
    } catch {
      case e: Exception => println(s"❌ Failed to send to Kafka: ${e.getMessage}")
    }
  }

  def fetchAndSendAll(categories: List[String]): Unit = {
    for (category <- categories) {
      println(s"📡 Fetching $category news...")
      val articles = fetchTopHeadlines(category)
      
      articles.foreach { article =>
        val articleJson = transformArticle(article, category)
        val timestamp = Instant.now.toEpochMilli
        sendToKafka(articleJson, category, timestamp)
        Thread.sleep(Config.ARTICLE_PROCESSING_DELAY_MS) // Fixed: Now defined
      }

      // Wait between API calls to respect rate limits
      if (categories.last != category) { // Don't wait after the last category
        Thread.sleep(Config.API_CALL_DELAY_MS) // Fixed: Now defined
        println(s"⏱️  Waiting ${Config.API_CALL_DELAY_MS}ms before next category...")
      }
    }
  }
}

object NewsProducerApp {

  def main(args: Array[String]): Unit = {
    
    // Display configuration
    Config.displayConfig()
    
    val props = new Properties()
    props.put("bootstrap.servers", Config.KAFKA_BOOTSTRAP_SERVERS)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "all")
    props.put("retries", "3")
    
    val producer = new KafkaProducer[String, String](props)
    val fetcher = new NewsFetcher(producer, "news-api-data")

    println(s"🚀 Starting continuous news fetching every ${Config.FETCH_INTERVAL_MINUTES} minutes")
    println(s"📰 Categories: ${Config.CATEGORIES}")

    val timer = new Timer()
    val task = new TimerTask {
      def run(): Unit = {
        try {
          fetcher.fetchAndSendAll(Config.CATEGORIES)
        } catch {
          case e: Exception => println(s"❌ Error during fetch loop: ${e.getMessage}")
        }
      }
    }

    // Run immediately, then schedule periodic execution
    fetcher.fetchAndSendAll(Config.CATEGORIES)
    timer.schedule(task, Config.FETCH_INTERVAL_MS, Config.FETCH_INTERVAL_MS) // Fixed: Use Long milliseconds

    println("⏳ Producer running... Press Ctrl+C to stop")
    
    sys.addShutdownHook {
      println("🛑 Shutting down gracefully...")
      producer.close()
      timer.cancel()
    }
    
    // Keep the main thread alive
    while (true) {
      Thread.sleep(1000)
    }
  }
}