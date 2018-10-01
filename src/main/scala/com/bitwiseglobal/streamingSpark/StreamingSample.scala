package com.bitwiseglobal.streamingSpark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.functions._


/**
  * Created by shivarajn on 6/13/2018.
  */
object StreamingSample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("StreamingSample").master("local")
      .config("spark.sql.warehouse.dir", "file:///tmp")
      .config("spark.sql.shuffle.partitions", 200)
      .getOrCreate()

    spark.sparkContext.setLogLevel("OFF")

    /*val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))

    val lines = ("C:\\Users\\shivarajn\\IdeaProjects\\NativeSparkProject\\src\\input\\jsonInput")

    //val lines = ssc.socketTextStream("localhost", 9999)

    val words = lines.flatMap(_.split(" "))

    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    println("############################")
    lines.print()
    wordCounts.print()
    println("############################")

    ssc.start()

    ssc.awaitTermination()*/

    //val schemaJson = spark.read.json("C:\\Users\\shivarajn\\IdeaProjects\\NativeSparkProject\\src\\input\\jsonInput")


    //val schemaJson = spark.read.json("C:\\Users\\shivarajn\\IdeaProjects\\NativeSparkProject\\src\\input\\jsonInput")

    //schemaJson.printSchema()
    val schema = StructType(List(
      StructField("eventTime", TimestampType, nullable = true),
      StructField("state", StringType, nullable = true)
    ))

    val jsonDf = spark.readStream.schema(schema).option("path","C:\\Users\\shivarajn\\IdeaProjects\\NativeSparkProject\\src\\input\\jsonInput").format("json").load()

    val watermarkJsonDf = jsonDf.withWatermark("eventTime", "10 minutes")

    val aggregateJsonDf = watermarkJsonDf.groupBy("eventTime").count()

    //val aggregateJsonDf = jsonDf.withWatermark("eventTime", "10 minutes").groupBy(window(jsonDf.col("eventTime"),"10 minutes","5 minutes")).count()

   //jsonDf.printSchema()

    //jsonDf.writeStream.outputMode("Append").format("json").start().awaitTermination()

    aggregateJsonDf.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

    /*watermarkJsonDf.writeStream.outputMode("append").option("checkpointLocation","C:\\Users\\shivarajn\\IdeaProjects\\NativeSparkProject\\src\\input\\jsonCheckpoint")
      .option("path","C:\\Users\\shivarajn\\IdeaProjects\\NativeSparkProject\\src\\input\\jsonOutput")
      .format("json").start().awaitTermination(10000)*/


  }

}
