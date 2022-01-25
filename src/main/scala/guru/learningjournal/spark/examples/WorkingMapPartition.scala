package guru.learningjournal.spark.examples

package guru.learningjournal.spark.examples

import org.apache.log4j.Logger
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, from_json, to_timestamp, window}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions._


object WorkingMapPartition {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args:Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Stream Stream Join Demo")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.shuffle.partitions", 2)
      .getOrCreate()


    val impressionSchema = StructType(List(
      StructField("InventoryID", StringType),
      StructField("CreatedTime", StringType),
      StructField("Campaigner", StringType)
    ))

    val clickSchema = StructType(List(
      StructField("InventoryID", StringType),
      StructField("CreatedTime", StringType)
    ))
    val kafkaImpressionDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "impressions")
      .option("startingOffsets", "latest")
      .load()

    val impressionsDF = kafkaImpressionDF
      .select(from_json(col("value").cast("string"), impressionSchema).alias("value"))
      .selectExpr("value.InventoryID as ImpressionID", "value.CreatedTime", "value.Campaigner")
      .withColumn("ImpressionTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss"))
      .drop("CreatedTime")

    import spark.implicits._
    import spark.sparkContext.implicits._


   /* val df4 = impressionsDF.mapPartitions(iterator => {
      //val util = new Util()
      val res = iterator.map(row=>{
        val fullName = util.combine(row.getString(0),row.getString(1),row.getString(2))
        (fullName, row.getString(3),row.getInt(5))
      })
      res
    })*/

    impressionsDF.writeStream
      .queryName("events_per_window")
      .format("console")
      .outputMode("update")
      .start()
      .awaitTermination()


  }

/*  def unique_values(iterable) {
   val it = iter(iterable)
    previous = next(it)
    yield previous
    for item in it:
    if item != previous:
      previous
    = item
    yield item
  }*/

/*  def drop_duplicates(xs : DataFrame) = {
    var prev = None
    for x <-- xs
    if prev is None or abs(x - prev) > threshold:
    yield x
    prev = x
  }*/
}

