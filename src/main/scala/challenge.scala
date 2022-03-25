import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.desc
//import org.apache.spark.sql.functions.{array, col, collect_list, desc}
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
    val df_aux = df.filter(col("Rating") >= lowerThreshold)                                                      //Apply lower threshold to rating
      .filter(col("Rating") <= upperThreshold)                                                                 //Limit with upper threshold
      .sort(desc("Rating"))                                                                                 //Sort by desc order

    val df_2 = df_aux.coalesce(1).write.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "§")
      .mode("overwrite")
      .save("src/main/resources/best-apps.csv")

    //df_2.show(10000)
  }

  //PART 3
  def part3(df: DataFrame): Unit ={
    val df_aux = df.groupBy("App")                                                    //Group by app name
      .agg(collect_set("Category").as("Categories"),                      //Set will avoid duplicate categories
        first("Rating").cast("string").as("Rating"),
        max("Reviews").cast("long").as("Reviews"),
        first("Size").as("Size"),
        first("Installs").cast("string").as("Installs"),
        first("Type").cast("string").as("Type"),
        first("Price").cast("double").multiply(0.9).as("Price"),
        first("Content Rating").cast("string").as("Content_Rating"),
        first("Genres").as("Genres"),
        first("Last Updated").cast("String").as("Last_Updated"),
        first("Current Ver").cast("string").as("Current_Version"),
        first("Android Ver").cast("string").as("Minimum_Android_Version")
        )

    val df_3 = df_aux.withColumn("Size",
        when(col("Size").like("%k"), (regexp_replace(col("Size"), "k", "").cast("double")*0.001))
      .when(col("Size").like("%M"), (regexp_replace(col("Size"), "M", "").cast("double")))
      .otherwise(regexp_replace(col("Size"), " ", "").cast("double")))
      .withColumn("Genres", split(col("Genres"), ";").cast("array<string>"))
      .withColumn("Last_Updated", date_format(to_date(col("Last_Updated"), "MMMM dd, yyyy"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("Minimum_Android_Version", when(col("Minimum_Android_Version").like("%and up"), (regexp_replace(col("Minimum_Android_Version"), " and up", "")))
        .otherwise(regexp_replace(col("Minimum_Android_Version"), "", "")))

    df_3.show(10000)
  }

  def part4(): Unit ={

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

    val df_part1 = part1(df)
    val df_part2 = part2(df2)
    val df_part3 = part3(df2)

  }

}
