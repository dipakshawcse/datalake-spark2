import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  *
  */

object MergeTwoDataframe {
  def main(args: Array[String]): Unit = {
    val sourceTable = args(0)
    val filePath = args(1)

    // Spark Configuration set up
    val config = new SparkConf().setAppName("Merge Two Dataframes")
    config.set("spark.driver.allowMultipleContexts", "true")
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

      // Load data from CSV file
      val csvDf = spark.read.format("csv")
                            .option("header", "true")
                            .load(filePath)
      csvDf.show()

      // Merge both dataframes
      val mergeDf = sourceDf.union(csvDf)
      mergeDf.show

      // Dump final dataframe into hive target table
      mergeDf.write.mode(saveMode = "append").saveAsTable("bdp.employee")
    }
  }
}
