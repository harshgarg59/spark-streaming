package com.inn.garg

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions.{from_avro, to_avro}
import org.apache.spark.sql.functions.{col, expr, from_json, struct, sum, to_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object KafkaAvroSourceStreaming extends Serializable {
  @transient lazy val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("streaming is started")
    val spark = SparkSession.builder()
                            .appName("Kafka Based Streaming")
                            .master("local[4]")
                            .config("spark.streaming.stopGracefullyOnShutdown", "true")
                            .config("spark.shuffle.partition", "20")
                            .getOrCreate()


    val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092")
                  .option("subscribe", "notification_avro")
                  .option("startingOffset", "earliest")
                  .load()
    val schema="""|{
                  |  "type": "record",
                  |  "name": "InvoiceItem",
                  |  "namespace": "com.inn.garg.types",
                  |  "fields": [
                  |    {"name": "InvoiceNumber","type": ["string", "null"]},
                  |    {"name": "CreatedTime","type": ["long", "null"]},
                  |    {"name": "StoreID","type": ["string", "null"]},
                  |    {"name": "PosID","type": ["string", "null"]},
                  |    {"name": "CustomerType","type": ["string", "null"]},
                  |    {"name": "CustomerCardNo","type": ["string", "null"]},
                  |    {"name": "DeliveryType","type": ["string", "null"]},
                  |    {"name": "City","type": ["string", "null"]},
                  |    {"name": "State","type": ["string", "null"]},
                  |    {"name": "PinCode","type": ["string", "null"]},
                  |    {"name": "ItemCode","type": ["string", "null"]},
                  |    {"name": "ItemDescription","type": ["string", "null"]},
                  |    {"name": "ItemPrice","type": ["double", "null"]},
                  |    {"name": "ItemQty","type": ["int", "null"]},
                  |    {"name": "TotalValue","type": ["double", "null"]}
                  |  ]
                  |}""".stripMargin
    val valueDF = df.select(from_avro(col("value"), schema).alias("value"))

    val explodeDF = valueDF.filter("value.CustomerType ='PRIME'").groupBy("value.CustomerCardNo")
                           .agg(sum("value.TotalValue").alias("TotalPurchase"), sum(expr("value.TotalValue *2 ")
                             .cast("Integer")).alias("AggregatedRewards"))
    val kafkaDF = explodeDF.select(expr("CustomerCardNo as Key"), to_json(struct("*")).alias("value"))


    val invoiceWriterQuery = kafkaDF.writeStream
                                          .format("kafka")
                                          .queryName("Flattened Invoice Avro Writer2")
                                          .outputMode("update")
                                          .option("kafka.bootstrap.servers", "localhost:9092")
                                          .option("topic", "AggregatedRewards")
                                          .option("startingOffset", "earliest")
                                          .option("checkpointLocation", "checkpoint7")
                                          .start()
    invoiceWriterQuery.awaitTermination()


  }

}
