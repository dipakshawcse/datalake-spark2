import java.security.MessageDigest
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object UDFInSpark {
  def main(args: Array[String]): Unit = {
    // To handle ERROR SparkContext: Error initializing SparkContext.
    val config = new SparkConf().setAppName("UDF in Spark")
    config.set("spark.driver.allowMultipleContexts", "true")

    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("UDF in Spark")
      .config("spark.master", "local")
      .getOrCreate()

    // Create a UDF
    val md5Value = (input: String) => {
      MessageDigest.getInstance("MD5").digest(input.getBytes)
    }

    // Register UDF to use
     spark.udf.register("getMd5Value", md5Value)

    val md5ValueFunc = udf((input: String) => {
      MessageDigest.getInstance("MD5").digest(input.getBytes)
    }, ArrayType(ByteType))

    try {
      val empDf = spark.read.option("header", "true").csv("./src/main/resources/emp_data1.csv")
      empDf.createOrReplaceTempView("empTbl")

      val udfInQuery = spark.sql("select ename, md5Value(ename) as md5 from empTbl")
      udfInQuery.show(false)

      // Using UDF without registering
      val udfWithoutReg = empDf.select(col("ename"), md5ValueFunc(col("ename")))
      udfWithoutReg.show(false)
    } catch {
      case e: Exception => {
        println(e.printStackTrace())
      }
    }
  }
}