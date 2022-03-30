package com.sparkchallenge

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object challenge {

  //PART 1
  def part1(df: DataFrame): DataFrame = {
    val df_1 = df.groupBy("App").agg(round(avg(df("Sentiment_Polarity")), 5).as("Average_Sentiment_Polarity")) //Group by app name with avg sentiment polarity
      .na.fill(0) //Fill null with 0

    println("PART 1:")
    df_1.show(200)
    df_1
  }

  //PART 2
  def part2(df: DataFrame): Unit = {
    val lowerThreshold = 4.0
    val upperThreshold = 5.0
    val df_2aux = df.filter(col("Rating") >= lowerThreshold) //Apply lower threshold to rating
      .filter(col("Rating") <= upperThreshold) //Limit with upper threshold
      .sort(desc("Rating")) //Sort by desc order

    val df_2 = df_2aux.coalesce(1).write.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "§")
      .mode("overwrite")
      .save("src/main/resources/best_apps.csv")

    println("PART 2:")
    df_2aux.show(200)
  }

  //PART 3
  def part3(df: DataFrame): DataFrame = {
    val df_aux = df.groupBy("App") //Group by app name
      .agg(collect_set("Category").as("Categories"), //Set will avoid duplicate categories
        max("Reviews").cast("long").as("Reviews") //Filter row with max reviews
      )

    val df_3_aux = df_aux.join(df.drop("Category"), Seq("App", "Reviews"))

    val df_3 = df_3_aux.withColumn("Reviews", df_3_aux("Reviews").cast("long"))
      .withColumn("Size",
        when(col("Size").like("%k"), regexp_replace(col("Size"), "k", "").cast("double") * 0.001) //Convert kb to Mb (1/1000)
          .when(col("Size").like("%M"), regexp_replace(col("Size"), "M", "").cast("double")) //and remove letters k,M
          .otherwise(regexp_replace(col("Size"), " ", "").cast("double"))) //do nothing otherwise
      .withColumn("Price",
        when(col("Price").startsWith("$"), round(regexp_replace(col("Price"), "[$,]", "").cast("double") * 0.9, 2))
          .otherwise(regexp_replace(col("Price"), "", "")))
      .withColumnRenamed("Content Rating", "Content_Rating")
      .withColumn("Genres", split(col("Genres"), ";").cast("array<string>")) //Convert to array with delimiter ";"
      .withColumn("Last Updated", date_format(to_date(col("Last Updated"), "MMMM dd, yyyy"), "yyyy-MM-dd HH:mm:ss")) //Convert to date in new format
      .withColumnRenamed("Last Updated", "Last_Updated")
      .withColumnRenamed("Current Ver", "Current_Version")
      .withColumn("Android Ver",
        when(col("Android Ver").like("%and up"), regexp_replace(col("Android Ver"), " and up", "")) //Remove text and keep version
          .otherwise(regexp_replace(col("Android Ver"), "", ""))) //otherwise do nothing
      .withColumnRenamed("Android Ver", "Minimum_Android_Version")

    println("PART 3:")
    df_3.select("App", "Categories", "Rating",	"Reviews", "Size", "Installs", "Type", "Price",
      "Content_Rating",	"Genres",	"Last_Updated",	"Current_Version",	"Minimum_Android_Version").show(200)
    df_3
  }

  def part4(df_1: DataFrame, df_3: DataFrame): DataFrame = {
    val df_4 = df_1.join(df_3, Seq("App"), "inner") //Inner join on "App"

    df_4.coalesce(1).write.option("compression", "gzip").mode("overwrite").parquet("src/main/resources/googleplaystore_cleaned") //gzip compression

    println("PART 4:")
    df_4.select("App", "Categories", "Rating",	"Reviews", "Size", "Installs", "Type", "Price",
      "Content_Rating",	"Genres",	"Last_Updated",	"Current_Version",	"Minimum_Android_Version", "Average_Sentiment_Polarity").show(200)
    df_4
  }

  def part5(df_4: DataFrame): Unit = {
    val df_5_aux = df_4.withColumn("Genres", explode(col("Genres"))) //Explode arrays to separate multiple genres

    val df_5 = df_5_aux.groupBy("Genres").count() //Group by genre count
      .join(df_5_aux.groupBy("Genres").agg(round(avg("Rating"), 3).as("Average_Rating")), Seq("Genres")) //Join on genres to calculate avg rating
      .join(df_5_aux.groupBy("Genres").agg(round(avg("Average_Sentiment_Polarity"), 3).as("Average_Sentiment_Polarity")), Seq("Genres")) //Join on genres to calculate avg sentiment
      .withColumnRenamed("Genres", "Genre")

    df_5.coalesce(1).write.option("compression", "gzip").mode("overwrite").parquet("src/main/resources/googleplaystore_metrics") //gzip compression

    println("PART 5:")
    df_5.show(200)
  }

  def main(args: Array[String]): Unit = {
    //Create session
    val spark = SparkSession.builder.master("local").getOrCreate()

    //Create DataFrames from csv
    val df = spark.read
      .option("header", "true") //Include first line as header
      .option("escape", "\"") //Quotes are used as an escape character for some App titles, otherwise commas aren't ignored
      .option("nullValue", "nan") //Assign null to "nan" values
      .csv("src/main/resources/googleplaystore_user_reviews.csv") //CSV path
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

    spark.stop()
  }

}
