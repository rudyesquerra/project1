import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object Main {

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

    if (spark.catalog.tableExists("burritos_data") && spark.catalog.tableExists("burritos_location")) {
      print("Enter username: ")
      val userInput = readLine()

//      println(userInput)
//      println(userPassword)
      val cdf = spark.read.csv("credentials.csv")
//      cdf.show()
      val filterUser = cdf.filter((s"_c0 = '$userInput'"))
      val checkUser = filterUser.count()
//      val filterPassword = cdf.filter((s"_c1 = '$userPassword'"))
//      val checkPassword = filterPassword.count()
//      cdf.filter((s"_c0 = '$userInput'")).filter(s"_c1 = '$userPassword'").show()
//      cdf.filter((s"_c0 = '$userPassword'")).
//
//      //      cdf.filter(s"_c0 = '$userInput'").filter(s"_c1 = '$userPassword'")
      if(checkUser > 0){
        println("User Exists")
        print("Enter password: ")
        val userPassword = readLine()
        if(filterUser.select("_c1").where(s"_c1 == '${userPassword}'").count() == 1){
          println("Password entered correctly")
        } else {
          println("Wrong password")
        }
      } else {
        println("User not found")
      }

    } else {
          spark.sql("create table IF NOT EXISTS credentials(username varchar(100), password varchar(50))")
          spark.sql("INSERT INTO credentials VALUES('admin','admin'),('basic','basic')")
        //      spark.sql("ALTER table credentials SET TBLPROPERTIES('skip.header.line.count'='1')")
          spark.sql("select * from credentials").show()

          spark.sql("create table IF NOT EXISTS burritos_data(id Int, location String, btype String, date Date, neighborhood String, address String, url String, yelp Float, google Float, chips String, cost Float, hunger Float, mass Int, density Double, length Float, circum Float, volume Float, tortilla Float, temp Float, meat Float, fillings Float, meat_filling Float, uniformity Float, salsa_quality Float, synergy Float, wrap Float, overall Float, rec String, reviewer String, notes String, unreliable String, nonsd String, beef String, pico String, guac String, cheese String, fries String, sourc String, pork String, chicken String, shrimp String, fish String, rice String, beans String, lettuce String, tomato String, bpepper String, carrots String, cabbage String, sauce String, salsa String, cilantro String, onion String, taquito String, pineapple String, ham String, chile String, nopales String, lobster String, queso String, egg String, mushroom String, bacon String, sushi String, avocado String, corn String, zucchini String) row format delimited fields terminated by ',' ")
          spark.sql("LOAD DATA LOCAL INPATH 'Burritos1.csv' INTO TABLE burritos_data ")
          spark.sql("SELECT * FROM burritos_data").show(300, false)

          spark.sql("create table IF NOT EXISTS burritos_location(id_Number Int, location String, yelp Float, google Float, average Float) row format delimited fields terminated by ',' ")
          spark.sql("LOAD DATA LOCAL INPATH 'Burritos1_avg.csv' INTO TABLE burritos_location")
          spark.sql("SELECT * FROM burritos_location ORDER BY location ASC").show(500, false)
    }



//    println("What restaurant has the less expensive burrito with the best yelp review.")
//    spark.sql("SELECT location, btype, neighborhood, yelp, google, cost FROM burritos_data WHERE yelp > 4 AND neighborhood !='Houston' ORDER BY cost ASC LIMIT 5").show()

//    println("Which top three neighborhoods offer the burritos with google and yelp reviews combined with an average of 4.5+")
//    spark.sql("SELECT DISTINCT bavg.location, bavg.average, btotal.neighborhood FROM burritos_location bavg LEFT JOIN burritos_data btotal ON (bavg.id_Number = btotal.id) ORDER BY average DESC LIMIT 5").show(200 , false)

//    println("What are the top five burrito restaurants where the combination of meat and salsa average 4+")
//    spark.sql("SELECT location, btype, neighborhood, meat, salsa_quality FROM burritos_data WHERE meat > 4 AND salsa_quality > 4 ORDER BY meat DESC").show(false)

//    println("What is the place with the burrito has an overall qualification by the reviewer of 4+ which includes pico de gallo an guac")
//    spark.sql("SELECT DISTINCT location, btype, neighborhood, overall, pico, guac FROM burritos_data WHERE overall > 4 AND pico='x' AND guac='x' AND UPPER(location) NOT LIKE UPPER('%Taco Stand%') ORDER BY overall DESC").show(false)

//    println("What is the average cost of a burrito in a particular neighborhood.")
//    spark.sql("SELECT AVG(cost) FROM burritos_data WHERE neighborhood='Hillcrest'").show(false)

//    println("What type of meatless burrito has the worst overall qualifications.")
//    spark.sql("SELECT location, btype, neighborhood, cost, overall FROM burritos_data WHERE beef='' AND pork='' AND chicken='' AND shrimp='' AND fish='' AND overall IS NOT NULL ORDER BY overall ASC").show(false)

  }
}
