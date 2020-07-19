import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ConvertSchemaToDF {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf()
    config.set("spark.driver.allowMultipleContexts", "true")

    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("Convert Schema to DF")
      .config("spark.master", "local")
      .getOrCreate()

    try {
      val empDf = spark.read.option("header", "true").option("inferSchema", "true").csv("./src/main/resources/emp_data1.csv")
      empDf.createOrReplaceTempView("empTbl")

      import spark.sqlContext.implicits._
      val metaSchema = empDf.schema.prettyJson
      val schmeaDataset = spark.createDataset(metaSchema :: Nil)
      val schemaDf = spark.read.json(schmeaDataset)
      schemaDf.createOrReplaceTempView("schemaTempView")
      val schemaInTbl = spark.sql("SELECT exp.name, exp.type FROM schemaTempView" +
                                           " LATERAL VIEW explode(fields) explodeTbl as exp")
      schemaInTbl.show
    } catch {
      case e: Exception => {
        println(e.printStackTrace())
      }
    }
  }
}
