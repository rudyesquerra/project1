import org.apache.spark.sql.SparkSession

object test10{
  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    //System.setProperty("hadoop.home.dir", "C:\\winutils")
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")
    //val df2 = spark.read.csv("hdfs://localhost:9000/user/hive/warehouse/Bev_ConscountA.txt").collect().length
    val Bev_ConscountA=spark.read.csv("hdfs://localhost:9000/user/hive/warehouse/Bev_ConscountA.txt").toDF()
    Bev_ConscountA.createOrReplaceTempView("ConsA")
    spark.sql("SELECT * FROM ConsA").show()
  }
}
