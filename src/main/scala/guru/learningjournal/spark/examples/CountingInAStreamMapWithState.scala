package guru.learningjournal.spark.examples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, to_timestamp}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object CountingInAStreamMapWithState {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args:Array[String]): Unit = {
    val host = args(0)
    val port = args(1)
    val checkpointFolder = args(2)

    val isLocal = true

    val sparkSession = if (isLocal) {
      SparkSession.builder
        .master("local")
        .appName("my-spark-app")
        .config("spark.some.config.option", "config-value")
        .config("spark.driver.host","127.0.0.1")
        .config("spark.sql.parquet.compression.codec", "gzip")
        .master("local[3]")
        .getOrCreate()
    } else {
      SparkSession.builder
        .appName("my-spark-app")
        .config("spark.some.config.option", "config-value")
        .master("local[3]")
        .getOrCreate()
    }

    import sparkSession.implicits._

    val socketLines = sparkSession.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load()

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
      .option("startingOffsets", "earliest")
      .load()

    val impressionsDF = kafkaImpressionDF
      .select(from_json(col("value").cast("string"), impressionSchema).alias("value"))
      .selectExpr("value.InventoryID as ImpressionID", "value.CreatedTime", "value.Campaigner")
      .withColumn("ImpressionTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss"))
      .drop("CreatedTime")
    /*val messageDs = socketLines.as[String].
      flatMap(line => line.toLowerCase().split(" ")).
      map(word => WordCountEvent(word, 1))
*/
    // Generate running word count
/*
    val wordCounts = impressionsDF.as[InputRow].groupByKey(_.ImpressionID).
      mapGroupsWithState[WordCountInMemory, WordCountReturn](GroupStateTimeout.ProcessingTimeTimeout) {

        case (word: String, events: Iterator[WordCountEvent], state: GroupState[WordCountInMemory]) =>
          var newCount = if (state.exists) state.get.countOfWord else 0

          events.foreach(tuple => {
            newCount += tuple.countOfWord
          })
*/

      //   state.update(WordCountInMemory(newCount))

       //   WordCountReturn(word, newCount)
      }

    // Start running the query that prints the running counts to the console
   // val query = wordCounts.writeStream
 //     .outputMode("update")
   //   .format("console")
   //   .start()

  //  query.awaitTermination()
 // }
}

case class WordCountEvent(word:String, countOfWord:Int) extends Serializable {

}

case class WordCountInMemory(countOfWord: Int) extends Serializable {
}

case class WordCountReturn(word:String, countOfWord:Int) extends Serializable {

}
