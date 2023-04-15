package com.inn.garg

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object KafkaSourceStreaming extends Serializable {
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
      StructField("InvoiceNumber", StringType),
      StructField("CreatedTime", LongType),
      StructField("StoreID", StringType),
      StructField("PosID", StringType),
      StructField("CashierID", StringType),
      StructField("CustomerType", StringType),
      StructField("CustomerCardNo", StringType),
      StructField("TotalAmount", DoubleType),
      StructField("NumberOfItems", IntegerType),
      StructField("PaymentMethod", StringType),
      StructField("CGST", DoubleType),
      StructField("SGST", DoubleType),
      StructField("CESS", DoubleType),
      StructField("DeliveryType", StringType),
      StructField("DeliveryAddress", StructType(List(
        StructField("AddressLine", StringType),
        StructField("City", StringType),
        StructField("State", StringType),
        StructField("PinCode", StringType),
        StructField("ContactNumber", StringType)
      ))),
      StructField("InvoiceLineItems", ArrayType(StructType(List(
        StructField("ItemCode", StringType),
        StructField("ItemDescription", StringType),
        StructField("ItemPrice", DoubleType),
        StructField("ItemQty", IntegerType),
        StructField("TotalValue", DoubleType)
      )))),
    ))

    val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092")
                  .option("subscribe", "invoices")
                  .option("startingOffset", "earliest")
                  .load()
    val valueDF = df.select(from_json(col("value").cast("String"), schema).alias("value"))
    val explodeDF = valueDF.selectExpr("value.InvoiceNumber", "value.CreatedTime", "value.StoreID",
      "value.PosID", "value.CustomerType", "value.PaymentMethod", "value.DeliveryType", "value.DeliveryAddress.City",
      "value.DeliveryAddress.State", "value.DeliveryAddress.PinCode", "explode(value.InvoiceLineItems) as LineItem")


    val flattenedDF = explodeDF.withColumn("ItemCode", expr("LineItem.ItemCode"))
                  .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
                  .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
                  .withColumn("ItemQty", expr("LineItem.ItemQty"))
                  .withColumn("TotalValue", expr("LineItem.TotalValue"))
                  .drop("LineItem")
    val invoiceWriterQuery = flattenedDF.writeStream
                                        .format("json")
                                        .queryName("Flattened Invoice Writer")
                                        .outputMode("append")
                                        .option("path", "output3")
                                        .option("checkpointLocation", "checkpoint")
                                        .trigger(Trigger.ProcessingTime("1 minute"))
                                        .start()

    invoiceWriterQuery.awaitTermination()


  }

}
