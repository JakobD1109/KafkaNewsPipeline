import slick.jdbc.PostgresProfile.api._
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory

object DatabaseTest {
  
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global
  
  def main(args: Array[String]): Unit = {
    println(" Testing database connection...")
    
    val config = ConfigFactory.load()
    
    // Try different config paths
    println(" Checking configuration...")
    
    try {
      // Method 1: Use the alternative config
      println(" Attempting connection with 'mydb' config...")
      val db = Database.forConfig("mydb", config)
      testConnection(db)
      
    } catch {
      case e: Exception =>
        println(s" Method 1 failed: ${e.getMessage}")
        
        try {
          // Method 2: Manual configuration
          println(" Attempting manual configuration...")
          val db = Database.forURL(
            url = "jdbc:postgresql://localhost:54806/news_pipeline",
            user = "postgres",
            password = "12345678",
            driver = "org.postgresql.Driver"
          )
          testConnection(db)
          
        } catch {
          case e2: Exception =>
            println(s" Method 2 failed: ${e2.getMessage}")
            
            // Method 3: Direct connection test
            println(" Testing basic JDBC connection...")
            testJdbcConnection()
        }
    }
  }
  
  def testConnection(db: Database): Unit = {
    try {
      // Test basic connection
      println(" Testing database connection...")
      val future = db.run(sql"SELECT 1 as test_value".as[Int])
      val result = Await.result(future, 10.seconds)
      println(" Database connection successful!")
      println(s" Test query result: ${result.head}")
      
      // Test if news_articles table exists
      println(" Checking if news_articles table exists...")
      val tableCheckFuture = db.run(sql"""
        SELECT EXISTS (
          SELECT FROM information_schema.tables 
          WHERE table_schema = 'public' 
          AND table_name = 'news_articles'
        )
      """.as[Boolean])
      
      val tableExists = Await.result(tableCheckFuture, 5.seconds)
      if (tableExists.head) {
        println(" news_articles table exists!")
        
        // Count existing records
        val countFuture = db.run(sql"SELECT COUNT(*) FROM news_articles".as[Long])
        val count = Await.result(countFuture, 5.seconds)
        println(s" Current articles in database: ${count.head}")
      } else {
        println("  news_articles table does not exist!")
        println(" You may need to create the table first.")
      }
      
    } finally {
      println(" Closing database connection...")
      db.close()
      println(" Database connection closed.")
    }
  }
  
  def testJdbcConnection(): Unit = {
    try {
      import java.sql.DriverManager
      
      Class.forName("org.postgresql.Driver")
      val connection = DriverManager.getConnection(
        "jdbc:postgresql://localhost:54806/news_pipeline",
        "postgres",
        "12345678"
      )
      
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT 1")
      
      if (resultSet.next()) {
        println(" Direct JDBC connection successful!")
        println(s" Test result: ${resultSet.getInt(1)}")
      }
      
      resultSet.close()
      statement.close()
      connection.close()
      
    } catch {
      case e: Exception =>
        println(s" Direct JDBC connection failed: ${e.getMessage}")
        println(" Possible issues:")
        println("   - PostgreSQL not running on localhost:54806")
        println("   - Database 'news_pipeline' doesn't exist")
        println("   - Wrong username/password")
        println("   - Firewall blocking connection")
    }
  }
}
