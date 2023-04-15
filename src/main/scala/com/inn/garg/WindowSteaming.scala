package com.inn.garg

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, from_json, sum, to_timestamp, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object WindowSteaming extends Serializable {
  @transient lazy val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("streaming is started")
    val spark = SparkSession.builder()
                            .appName("Kafka Based Streaming")
                            .master("local[4]")
                            .config("spark.streaming.stopGracefullyOnShutdown", "true")
                            .config("spark.shuffle.partition", "20")
                            .getOrCreate()

    val schema = StructType(List(
      StructField("CreatedTime", StringType),
      StructField("Type", StringType),
      StructField("Amount", IntegerType),
      StructField("BrokerCode", StringType)
    ))

    val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092")
                  .option("subscribe", "trades")
                  .option("startingOffsets", "earliest")
                  .load()
    val valueDF = df.select(from_json(col("value").cast("String"), schema).alias("value"))
    val explodeDF = valueDF.select("value.*")


    val flattenedDF = explodeDF.withColumn("CreatedTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss"))
                               .withColumn("BUY",expr("case when Type=='BUY' then Amount else 0 end"))
                               .withColumn("SELL",expr("case when Type=='SELL' then Amount else 0 end"))

    val windowDF=flattenedDF.groupBy(window(col("CreatedTime"), "15 Minute"))
               .agg(sum("BUY").alias("TotalBuy"),sum("SELL").alias("TotalSell"))

    val finalWindowDF=windowDF.select("window.start","window.end","TotalBuy","TotalSell")

    val invoiceWriterQuery = finalWindowDF.writeStream
                                        .format("console")
                                        .queryName("Flattened Invoice Writer")
                                        .outputMode("update")
                                        .option("checkpointLocation", "checkpoint10")
                                        .trigger(Trigger.ProcessingTime("1 minute"))
                                        .start()

    invoiceWriterQuery.awaitTermination()


  }

}
