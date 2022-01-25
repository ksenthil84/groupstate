package guru.learningjournal.spark.examples

import org.apache.log4j.Logger
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, to_timestamp, window}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions._


object Working {
Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)

  case class InputRow(user:String, timestamp:java.sql.Timestamp, activity:String)
  case class UserState(user:String,
                       var activity:String,
                       var start:java.sql.Timestamp,
                       var end:java.sql.Timestamp)

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
  impressionsDF.printSchema()
  impressionsDF
  import spark.implicits._
  val outputDF=impressionsDF
    .selectExpr("ImpressionID as user", "ImpressionTime as timestamp", "Campaigner as activity")
    .as[InputRow]
    // group the state by user key
    .groupByKey(_.user)
    .mapGroupsWithState(GroupStateTimeout.NoTimeout)(updateAcrossEvents)
  outputDF.printSchema()
  val outputDF1= outputDF.withWatermark("end", "15 minute")
    .groupBy(col("user"),
      window(col("end"), "15 minute", "5 minute"))
    .agg(max("end").alias("end1"))

  outputDF1.writeStream
    .queryName("events_per_window")
    .format("console")
    .outputMode("update")
    .start()
    .awaitTermination()


}

  def updateUserStateWithEvent(state:UserState, input:InputRow):UserState = {
    // no timestamp, just ignore it
    if (Option(input.timestamp).isEmpty) {
      return state
    }
    //does the activity match for the input row
    if (state.activity == input.activity) {
      if (input.timestamp.after(state.end)) {
        state.end = input.timestamp
      }
      if (input.timestamp.before(state.start)) {
        state.start = input.timestamp
      }
    } else {
      //some other activity
      if (input.timestamp.after(state.end)) {
        state.start = input.timestamp
        state.end = input.timestamp
        state.activity = input.activity
      }
    }
    //return the updated state
    state
  }
  def updateAcrossEvents(user:String,
                         inputs: Iterator[InputRow],
                         oldState: GroupState[UserState]):UserState = {
    var state:UserState = if (oldState.exists) oldState.get else UserState(user,
      "",
      new java.sql.Timestamp(6284160000000L),
      new java.sql.Timestamp(6284160L)
    )
    // we simply specify an old date that we can compare against and
    // immediately update based on the values in our data

    for (input <- inputs) {
      state = updateUserStateWithEvent(state, input)
      oldState.update(state)
    }
    oldState.remove()
    state
  }
}

