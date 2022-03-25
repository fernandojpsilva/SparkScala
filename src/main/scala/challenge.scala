import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.desc
//import org.apache.spark.sql.functions.{array, col, collect_list, desc}
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}

object challenge {

  //PART 1
  def part1(df: DataFrame): DataFrame ={
    val df_1 = df.groupBy("App").agg(avg(df("Sentiment_Polarity")).as("Average_Sentiment_Polarity"))      //Group by app name with avg sentiment polarity
      .na.fill(0)                                                                                              //Fill null with 0

    println("PART 1:")
    df_1.show(200)
    df_1
  }

  //PART 2
  def part2(df: DataFrame): Unit ={
    val lowerThreshold = 4.0
    val upperThreshold = 5.0
    val df_2aux = df.filter(col("Rating") >= lowerThreshold)                             //Apply lower threshold to rating
      .filter(col("Rating") <= upperThreshold)                                          //Limit with upper threshold
      .sort(desc("Rating"))                                                          //Sort by desc order

    val df_2 = df_2aux.coalesce(1).write.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "§")
      .mode("overwrite")
      .save("src/main/resources/best-apps.csv")

    println("PART 2:")
    df_2aux.show(200)
  }

  //PART 3
  def part3(df: DataFrame): DataFrame={
    val df_aux = df.groupBy("App")                                                    //Group by app name
      .agg(collect_set("Category").as("Categories"),                      //Set will avoid duplicate categories
        first("Rating").cast("double").as("Rating"),
        max("Reviews").cast("long").as("Reviews"),                   //Filter row with max reviews
        first("Size").as("Size"),
        first("Installs").cast("string").as("Installs"),
        first("Type").cast("string").as("Type"),
        first("Price").cast("double").multiply(0.9).as("Price"),  //Convert to Euro (*0.9)
        first("Content Rating").cast("string").as("Content_Rating"),
        first("Genres").as("Genres"),
        first("Last Updated").cast("String").as("Last_Updated"),
        first("Current Ver").cast("string").as("Current_Version"),
        first("Android Ver").cast("string").as("Minimum_Android_Version")
        )

    val df_3 = df_aux.withColumn("Size",
        when(col("Size").like("%k"), (regexp_replace(col("Size"), "k", "").cast("double")*0.001))      //Convert kb to Mb (1/1000)
      .when(col("Size").like("%M"), (regexp_replace(col("Size"), "M", "").cast("double")))            //and remove letters k,M
      .otherwise(regexp_replace(col("Size"), " ", "").cast("double")))                                                //do nothing otherwise
      .withColumn("Genres", split(col("Genres"), ";").cast("array<string>"))                                            //Convert to array with delimiter ";"
      .withColumn("Last_Updated", date_format(to_date(col("Last_Updated"), "MMMM dd, yyyy"), "yyyy-MM-dd HH:mm:ss"))    //Convert to date in new format
      .withColumn("Minimum_Android_Version",
        when(col("Minimum_Android_Version").like("%and up"), (regexp_replace(col("Minimum_Android_Version"), " and up", "")))  //Remove text and keep version
        .otherwise(regexp_replace(col("Minimum_Android_Version"), "", "")))                                                 //otherwise do nothing

    println("PART 3:")
    df_3.show(200)
    df_3
  }

  def part4(df_1: DataFrame, df_3: DataFrame): DataFrame ={
    val df_4 = df_1.join(df_3, Seq("App"), "inner")                                               //Inner join on "App"

    df_4.write.option("compression", "gzip").mode("overwrite").parquet("src/main/resources/googleplaystore_cleaned")    //gzip compression

    println("PART 4:")
    df_4.show(200)
    df_4
  }

  def part5(df_4: DataFrame): Unit ={
    val df_5_aux = df_4.withColumn("Genres", explode(col("Genres")))      //Explode arrays to separate multiple genres

    val df_5 = df_5_aux.groupBy("Genres").count()                                      //Group by genre count
      .join(df_5_aux.groupBy("Genres").agg(avg("Rating").as("Average_Rating")), Seq("Genres"))   //Join on genres to calculate avg rating
      .join(df_5_aux.groupBy("Genres").agg(avg("Average_Sentiment_Polarity").as("Average_Sentiment_Polarity")), Seq("Genres"))  //Join on genres to calculate avg sentiment
      .withColumnRenamed("Genres", "Genre")

    df_5.write.option("compression", "gzip").parquet("src/main/resources/googleplaystore_metrics")    //gzip compression

    println("PART 5:")
    df_5.show(200)
  }

  def main(args: Array[String]): Unit =
  {
    //Create session
    val spark = SparkSession.builder.master("local").getOrCreate()

    //Create DataFrames from csv
    val df = spark.read
      .option("header", "true")                                                                      //Include first line as header
      .option("escape", "\"")                                                                        //Quotes are used as an escape character for some App titles, otherwise commas aren't ignored
      .option("nullValue", "nan")                                                                    //Assign null to "nan" values
      .csv("src/main/resources/googleplaystore_user_reviews.csv")                              //CSV path
    //df.show(1000)

    val df2 = spark.read
      .option("header", "true")
      .option("escape", "\"")
      .option("nullValue", "NaN")
      .csv("src/main/resources/googleplaystore.csv")

    val df_part1 = part1(df)
    part2(df2)
    val df_part3 = part3(df2)
    val df_part4 = part4(df_part1, df_part3)
    part5(df_part4)
  }

}
