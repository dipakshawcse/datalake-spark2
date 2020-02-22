import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object FullDataIngestion {
  def main(args: Array[String]): Unit = {
    val sourceTable = args(0)
    val targetTable = args(1)

    // Spark Configuration set up
    val config = new SparkConf().setAppName("Full Data Load " + sourceTable)
    config.set("spark.driver.allowMultipleContexts","true")
    val spark = SparkSession.builder().appName("Full Load").getOrCreate()

    try {
      print("Started.......")
      // JDBC connection details
      val driver = "com.mysql.jdbc.Driver"
      val url = "jdbc:mysql://localhost:3306/bdp"
      val user = "root"
      val pass = "Password"

      // JDBC Connection and load table in Dataframe
      val sourceDf = spark.read.format("jdbc")
        .option("driver", driver)
        .option("url", url)
        .option("dbtable", sourceTable)
        .option("user", user)
        .option("password", pass)
        .load()

      // Read data from Dataframe
      sourceDf.show()
      sourceDf.write.mode("append").saveAsTable(targetTable)

    } catch {
      case e : Throwable => println("Connectivity Failed for Table ", e)
    }
  }
}
