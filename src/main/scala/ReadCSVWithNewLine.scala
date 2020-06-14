import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}

object ReadCSVWithNewLine {
  def main(args: Array[String]): Unit = {

    // To handle ERROR SparkContext: Error initializing SparkContext.
    val config = new SparkConf().setAppName("Read CSV")
    config.set("spark.driver.allowMultipleContexts", "true")

    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("Read CSV with new line")
      .config("spark.master", "local")
      .getOrCreate()

    try{
      val empDf = spark.read.csv("./src/main/resources/emp_data_with_newline.csv")
      empDf.show()

      val empDfWithOption = spark.read.option("header", "true")
        .option("multiLine", "true")
        .csv("./src/main/resources/emp_data_with_newline.csv")
      empDfWithOption.show()
    }
  }
}