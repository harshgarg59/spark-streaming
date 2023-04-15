package com.inn.garg

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, flatten}
import org.apache.spark.sql.streaming.Trigger

object FileBasedStreaming extends Serializable {
  @transient lazy val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("streaming is started")
    val spark = SparkSession.builder()
                            .appName("File Based Streaming")
                            .master("local[4]")
                            .config("spark.streaming.stopGracefullyOnShutdown", "true")
                            .config("spark.shuffle.partition", "20")
                            .config("spark.sql.streaming.schemaInference", "true")
                            .getOrCreate()

    val df = spark.readStream
                  .format("json")
                  .option("path", "input")
                  .option("cleanSource", "archive")
                  .option("sourceArchiveDir", "archiveDir")
                  .load()
    val explodedDF = df.selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "PosID", "CustomerType",
      "PaymentMethod", "DeliveryType", "DeliveryAddress.City", "DeliveryAddress.State", "DeliveryAddress.PinCode", "explode(InvoiceLineItems) as LineItem")

    val flattenDF = explodedDF.withColumn("ItemCode", col("LineItem.ItemCode"))
                              .withColumn("ItemDescription", col("LineItem.ItemDescription"))
                              .withColumn("ItemPrice", col("LineItem.ItemPrice"))
                              .withColumn("ItemQty", col("LineItem.ItemQty"))
                              .withColumn("TotalValue", col("LineItem.TotalValue")).drop(col("LineItem"))
    val finalQuery = flattenDF.writeStream.format("json").option("path", "output1")
                              .option("checkpointLocation", "checkpoint1")
                              .queryName("File Based Query").trigger(Trigger.ProcessingTime("1 Minute"))
                              .outputMode("Append")
                              .start()
    logger.info("streaming is Finished")
    finalQuery.awaitTermination()


  }

}
