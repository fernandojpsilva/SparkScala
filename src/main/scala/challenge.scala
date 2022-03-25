import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, desc}
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}

object challenge {

  //PART 1
  def part1(df: DataFrame): Unit ={
    val df_1 = df.groupBy("App","Sentiment_Polarity").avg()                                                 //Group by app name with avg sentiment polarity
      .withColumnRenamed("Sentiment_Polarity", "Average_Sentiment_Polarity")                      //Rename column
      .na.fill("0")                                                                                              //Fill null with 0

    println("PART 1:")
    df_1.show()
  }

  //PART 2
  def part2(df: DataFrame): Unit ={
    val lowerThreshold = 4.0
    val upperThreshold = 5.0
    val df_2 = df.filter(col("Rating") >= lowerThreshold)                                                      //Apply lower threshold to rating
      .filter(col("Rating") <= upperThreshold)                                                                 //Limit with upper threshold
      .sort(desc("Rating"))                                                                                 //Sort by desc order

    df_2.coalesce(1).write.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "§")
      .mode("overwrite")
      .save("src/main/resources/best-apps.csv")

    //df_2.show(10990)
  }

  def main(args: Array[String]): Unit =
  {
    //Create session
    val spark = SparkSession.builder.master("local").getOrCreate()

    //Create DataFrames from csv
    val df = spark.read
      .option("header", "true")                                                                                         //Include first line as header
      .option("escape", "\"")                                                                                           //Quotes are used as an escape character for some App titles, otherwise commas aren't ignored
      .option("nullValue", "nan")                                                                                       //Assign null to "nan" values
      .csv("src/main/resources/googleplaystore_user_reviews.csv")                                                 //CSV path
    //df.show(1000)

    val df2 = spark.read
      .option("header", "true")
      .option("escape", "\"")
      .option("nullValue", "NaN")
      .csv("src/main/resources/googleplaystore.csv")

    //part1(df)
    part2(df2)

  }

}
