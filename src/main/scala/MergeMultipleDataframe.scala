import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object MergeMultipleDataframe {
  def customSelect(availableCols: Set[String], requiredCols: Set[String]) = {
    requiredCols.toList.map(column => column match {
      case column if availableCols.contains(column) => col(column)
      case _ => lit(null).as(column)
    })
  }

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setAppName("Merge Two Dataframes")
    config.set("spark.driver.allowMultipleContexts", "true")
    val spark = SparkSession.builder()
                            .appName("Merge Multiple Dataframes")
                            .config("spark.master", "local")
                            .getOrCreate()
    try {
      //import spark.implicits._
      val emp_dataDf1 = spark.read.format("csv")
                                  .option("header", "true")
                                  .load("./src/main/resources/emp_data1.csv")
      val emp_dataDf2 = spark.read.format("csv")
                                  .option("header", "true")
                                  .load("./src/main/resources/emp_data2.csv")
      val emp_dataDf3 = spark.read.format("csv")
                                  .option("header", "true")
                                  .load("./src/main/resources/emp_data3.csv")
      emp_dataDf3.show()
      emp_dataDf1.show()
      emp_dataDf2.show()

      // Schema fixes
      val emp_data1Cols = emp_dataDf1.columns.toSet
      val emp_data2Cols = emp_dataDf2.columns.toSet
      val emp_data3Cols = emp_dataDf3.columns.toSet
      val requiredColumns = emp_data1Cols ++ emp_data2Cols ++ emp_data3Cols // union

      val empDf1 = emp_dataDf1.select(customSelect(
        emp_data1Cols,
        requiredColumns):_*)
      val empDf2 = emp_dataDf2.select(customSelect(
        emp_data2Cols,
        requiredColumns): _*)
      val empDf3 = emp_dataDf3.select(customSelect(
        emp_data3Cols,
        requiredColumns): _*)

      // Approach 1
      val mergeDf = empDf1.union(empDf2).union(empDf3)
      mergeDf.show()

      // Approach 2
      val dfSeq = Seq(empDf1, empDf2, empDf3)
      val mergeSeqDf = dfSeq.reduce(_ union _)
      mergeSeqDf.show()
    }
  }
}
