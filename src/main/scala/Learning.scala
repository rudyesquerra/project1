import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Learning {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    Logger.getLogger("org").setLevel(Level.ERROR) //Remove Errors in output

    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("Error") //Remove Errors in output

    println("created spark session")

    val sfFireFile="sf-fire-calls.csv"
    val fireDF = spark.read.option("header", "true")
      .csv(sfFireFile)
//    fireDF.show()
    val fewFireDF = fireDF.select("IncidentNumber", "AvailableDtTm", "CallType")
      .where("\"Calltype\" != \"Medical Incident\"")
    fewFireDF.show(5, false)

    fireDF
      .select("CallType")
      .where(col("CallType").isNotNull)
//      .agg(countDistinct('CallType))
      .show()
  }
}
