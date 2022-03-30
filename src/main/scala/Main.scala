import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import scala.Console.{BLUE, BOLD, RED, RESET, WHITE}

object Main {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    Logger.getLogger("org").setLevel(Level.ERROR) //Remove Errors in output

    val spark = SparkSession
      .builder
      .appName("SDBurrosApp")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("Error") //Remove Errors in output

    println("created spark session")

      if (spark.catalog.tableExists("burritos_data")
        && spark.catalog.tableExists("burritos_location")
        && spark.catalog.tableExists("credentials")
        && spark.catalog.tableExists("query_data")){

        /*In case you forget the credentials, run the following query which shows the table 'credentials'*/
//        println("credentials table: ")
//        spark.sql("select * from credentials").show()

      print("Enter username: ")
      val userInput = readLine()
      print("Enter password: ")
      val userPassword = readLine()

      val checkUser = spark.sql(s"SELECT * FROM credentials WHERE username='${userInput}' AND password='${userPassword}'").count()

      if(checkUser > 0) {
                  userInput match {
                    case "admin" => {
                      var continue = true
                      while (continue) {
                        try {
                        var cdfTable = spark.table("credentials").toDF()
                        var queryTable = spark.table("query_data").toDF()
                        println(s"${BOLD}Enter \n1 => ${BOLD}${RED}C${RESET}reate Own Query " +
                          s"\n2 => ${BOLD}${RED}R${RESET}ead Query From Menu " +
                          s"\n3 => ${BOLD}${RED}U${RESET}pdate Your Password " +
                          s"\n4 => ${BOLD}${RED}D${RESET}elete User " +
                          s"\n5 => Add User" +
                          s"\n'q' => Exit")
                        val option = readLine()
                        option match {
                          case "1" => {
                            println("Chose option 1")
                            println("Type your customized query: ")
                            println("Dont forget to include \"...\" !!!")
                            val query = readLine()
                            spark.sql("create table IF NOT EXISTS temp(query String)")
                            spark.sql(s"INSERT INTO temp VALUES($query)")
                            val result = query.replaceAll("\"", "")
                            spark.sql(s"${result}").show()
                            spark.sql("SELECT * from temp").show()
                          }
                          case "2" => {
                            println("Chose option 2")
                            println("Choose your desired query: " +
                              "\n1. What restaurant has the less expensive burrito with the best yelp review?" +
                              "\n2. Which top three neighborhoods offer the burritos with google and yelp reviews combined with an average of 4.5+?" +
                              "\n3. What are the top five burrito restaurants where the combination of meat and salsa average 4+?" +
                              "\n4. What is the place with the burrito has an overall qualification by the reviewer of 4+ which includes pico de gallo an guac?" +
                              "\n5. What is the average cost of a burrito in the Hillcrest neighborhood?" +
                              "\n6. What type of meatless burrito has the worst overall qualifications?")
                            val option = readInt()
                            option match {
                              case 1 => {
                                println("The restaurant with the less expensive burrito and best yelp review is: ")
                                spark.sql("SELECT DISTINCT location, btype, neighborhood, yelp, google, cost FROM burritos_data WHERE yelp > 4 AND neighborhood !='Houston' ORDER BY cost ASC LIMIT 5").show()
                              }
                              case 2 => {
                                println("The top three neighborhoods that offer the burritos with google and yelp reviews combined with an average of 4.5+ are: ")
                                spark.sql("SELECT DISTINCT bavg.location, bavg.average, btotal.neighborhood FROM burritos_location bavg LEFT JOIN burritos_data btotal ON (bavg.id_Number = btotal.id) ORDER BY average DESC LIMIT 3").show(200, false)
                              }
                              case 3 => {
                                println("The top five burrito restaurants where the combination of meat and salsa average 4+ are: ")
                                spark.sql("SELECT location, btype, neighborhood, meat, salsa_quality FROM burritos_data WHERE meat > 4 AND salsa_quality > 4 ORDER BY meat DESC LIMIT 5").show(false)
                              }
                              case 4 => {
                                println("The place with the burrito that has an overall qualification by the reviewer of 4+ which includes pico de gallo an guac is: ")
                                spark.sql("SELECT DISTINCT location, btype, neighborhood, overall, pico, guac FROM burritos_data WHERE overall > 4 AND pico='x' AND guac='x' AND UPPER(location) NOT LIKE UPPER('%Taco Stand%') ORDER BY overall DESC").show(false)
                              }
                              case 5 => {
                                println("The average cost of a burrito in the Hillcrest neighborhood is: ")
                                spark.sql("SELECT AVG(cost) FROM burritos_data WHERE neighborhood='Hillcrest'").show(false)
                              }
                              case 6 => {
                                println("The meatless burrito with the worst overall qualifications is: ")
                                spark.sql("SELECT DISTINCT location, btype, neighborhood, cost, overall FROM burritos_data WHERE beef='' AND pork='' AND chicken='' AND shrimp='' AND fish='' AND overall IS NOT NULL ORDER BY overall ASC").show(false)
                              }
                            }
                          }
                          case "3" => {
                            println("Chose option 3")
                            print("Enter a new password: ")
                            val newPassword = readLine()

                            val updatedCdf = cdfTable.withColumn("password", when(col("password") === s"$userPassword", s"$newPassword").otherwise(col("password")))
                            updatedCdf.show()
                            updatedCdf.write.mode("overwrite").saveAsTable("credentials2")
                            spark.sql("DROP TABLE IF EXISTS credentials")
                            spark.sql("ALTER TABLE credentials2 RENAME TO credentials")
                          }
                          case "4" => {
                            println("Chose option 4")
                            println("Enter username you want to delete")
                            val delete = readLine()
                            var cdfTableFiltered = cdfTable.filter(!col("username").isin(s"$delete"))
                            cdfTableFiltered.show()
                            cdfTableFiltered.write.mode("overwrite").saveAsTable("credentials3")
                            spark.sql("DROP TABLE IF EXISTS credentials")
                            spark.sql("ALTER TABLE credentials3 RENAME TO credentials")
                          }
                          case "5" => {
                            println("Chose option 5")
                            println("Type the username: ")
                            println("Dont forget to include \"...\" !!!")
                            val newUser = readLine()
                            println("Type the password: ")
                            val newPass = readLine()
                            spark.sql("DROP TABLE IF EXISTS adduser")
                            spark.sql("create table IF NOT EXISTS adduser(username String, password String)")
                            spark.sql(s"INSERT INTO adduser VALUES($newUser, $newPass)")
                            var cdfAdd = spark.table("adduser").toDF()
                            cdfTable.union(cdfAdd).write.mode("append").saveAsTable("credentials4")
                            spark.sql("DROP TABLE IF EXISTS credentials")
                            spark.sql("ALTER TABLE credentials4 RENAME TO credentials")
                            spark.sql("SELECT * FROM credentials").show()
                          }
                          case "q" => continue = false
                          case _ => println(s"${BOLD}${RED}ERROR:${WHITE} Unknown command ${RESET}")
                        }
                      }
                        catch {
                          case e: Exception => println("Not a valid option " + e)
                        }
                      }
                      println(s"\n${BOLD}${BLUE}See ya!\n")
                    }
                    case _ =>
                      var continue = true
                      while (continue) {
                        try {
                        println("Choose your desired query: " +
                          "\n1 => What restaurant has the less expensive burrito with the best yelp review?" +
                          "\n2 => Which top three neighborhoods offer the burritos with google and yelp reviews combined with an average of 4.5+?" +
                          "\n3 => What are the top five burrito restaurants where the combination of meat and salsa average 4+?" +
                          "\n4 => What is the place with the burrito has an overall qualification by the reviewer of 4+ which includes pico de gallo an guac?" +
                          "\n5 => What is the average cost of a burrito in the Hillcrest neighborhood?" +
                          "\n6 => What type of meatless burrito has the worst overall qualifications?" +
                          "\n'q' => Exit ")
                        val option = readLine()
                        option match {
                          case "1" => {
                            println("The restaurant with the less expensive burrito and best yelp review is: ")
                            spark.sql("SELECT DISTINCT location, btype, neighborhood, yelp, google, cost FROM burritos_data WHERE yelp > 4 AND neighborhood !='Houston' ORDER BY cost ASC LIMIT 5").show()
                          }
                          case "2" => {
                            println("The top three neighborhoods that offer the burritos with google and yelp reviews combined with an average of 4.5+ are: ")
                            spark.sql("SELECT DISTINCT bavg.location, bavg.average, btotal.neighborhood FROM burritos_location bavg LEFT JOIN burritos_data btotal ON (bavg.id_Number = btotal.id) ORDER BY average DESC LIMIT 5").show(200, false)
                          }
                          case "3" => {
                            println("The top five burrito restaurants where the combination of meat and salsa average 4+ are: ")
                            spark.sql("SELECT location, btype, neighborhood, meat, salsa_quality FROM burritos_data WHERE meat > 4 AND salsa_quality > 4 ORDER BY meat DESC").show(false)
                          }
                          case "4" => {
                            println("The place with the burrito that has an overall qualification by the reviewer of 4+ which includes pico de gallo an guac is: ")
                            spark.sql("SELECT DISTINCT location, btype, neighborhood, overall, pico, guac FROM burritos_data WHERE overall > 4 AND pico='x' AND guac='x' AND UPPER(location) NOT LIKE UPPER('%Taco Stand%') ORDER BY overall DESC").show(false)
                          }
                          case "5" => {
                            println("The average cost of a burrito in the Hillcrest neighborhood is: ")
                            spark.sql("SELECT AVG(cost) FROM burritos_data WHERE neighborhood='Hillcrest'").show(false)
                          }
                          case "6" => {
                            println("The meatless burrito with the worst overall qualifications is: ")
                            spark.sql("SELECT DISTINCT location, btype, neighborhood, cost, overall FROM burritos_data WHERE beef='' AND pork='' AND chicken='' AND shrimp='' AND fish='' AND overall IS NOT NULL ORDER BY overall ASC").show(false)
                          }
                          case "q" => continue = false
                        }
                      } catch {
                          case e: Exception => println("Not a valid option " + e)
                        }
                      }
                      println(s"\n${BOLD}${BLUE}See ya!\n")
                  }

      }
        else println(s"${BOLD}${RED}Incorrect User or Password!${RESET}")
      } else {
          var cdf = spark.read.option("header", true).csv("credentials.csv") //created schema reading from a csv file
          .withColumnRenamed("_c0","username")
          .withColumnRenamed("_c1","password")
          cdf.write.mode("overwrite").saveAsTable("credentials")

          spark.sql("create table IF NOT EXISTS query_data(query String)")

          spark.sql("create table IF NOT EXISTS burritos_data(id Int, location String, btype String, date Date, neighborhood String, address String, url String, yelp Float, google Float, chips String, cost Float, hunger Float, mass Int, density Double, length Float, circum Float, volume Float, tortilla Float, temp Float, meat Float, fillings Float, meat_filling Float, uniformity Float, salsa_quality Float, synergy Float, wrap Float, overall Float, rec String, reviewer String, notes String, unreliable String, nonsd String, beef String, pico String, guac String, cheese String, fries String, sourc String, pork String, chicken String, shrimp String, fish String, rice String, beans String, lettuce String, tomato String, bpepper String, carrots String, cabbage String, sauce String, salsa String, cilantro String, onion String, taquito String, pineapple String, ham String, chile String, nopales String, lobster String, queso String, egg String, mushroom String, bacon String, sushi String, avocado String, corn String, zucchini String) row format delimited fields terminated by ','")
          spark.sql("LOAD DATA LOCAL INPATH 'Burritos1.csv' INTO TABLE burritos_data ")

          spark.sql("create table IF NOT EXISTS burritos_location(id_Number Int, location String, yelp Float, google Float, average Float) row format delimited fields terminated by ',' ")
          spark.sql("LOAD DATA LOCAL INPATH 'Burritos1_avg.csv' INTO TABLE burritos_location")
    }
  }
}
