package com.example
import org.apache.spark.sql.{DataFrame, SparkSession}


object datacore {
  def readMongoCollection(spark: SparkSession, database: String, collection: String): DataFrame = {
    spark.read.format("com.mongodb.spark.sql.DefaultSource")
      .option("uri", s"mongodb+srv://soufianejami123:****@cluster0.ixzabjx.mongodb.net/$database.$collection")
      .load()
  }

  def client_p_a_a(args: Array[String]): (SparkSession, DataFrame, String) = {
    val spark = SparkSession.builder()
      .appName("mongod")
      .master("local[*]")
      .config("spark.mongodb.input.database", "client")
      .getOrCreate()

    val df = readMongoCollection(spark, "client", "client_pm_activite_adate")
    val collectionName = "client_pm_activite_adate"
    (spark, df, collectionName)
  }

  def client_b_a_a(args: Array[String]): (SparkSession, DataFrame, String) = {
    val spark = SparkSession.builder()
      .appName("mongod")
      .master("local[*]")
      .config("spark.mongodb.input.database", "client")
      .getOrCreate()

    val df = readMongoCollection(spark, "client", "client_contacts_adate")
    val collectionName = "client_contacts_adate"
    (spark, df, collectionName)
  }

  def client_c_a_a(args: Array[String]): (SparkSession, DataFrame, String) = {
    val spark = SparkSession.builder()
      .appName("mongod")
      .master("local[*]")
      .config("spark.mongodb.input.database", "client")
      .getOrCreate()

    val df = readMongoCollection(spark, "client", "client_changements_agences_adate")
    val collectionName = "client_changements_agences_adate"
    (spark, df, collectionName)
  }

  def lastday(args: Array[String]): (SparkSession, DataFrame, String) = {
    val spark = SparkSession.builder()
      .appName("mongod")
      .master("local[*]")
      .config("spark.mongodb.input.database", "client")
      .getOrCreate()

    val df = readMongoCollection(spark, "client", "lastday")
    val collectionName = "lastday"
    (spark, df, collectionName)
  }
}

