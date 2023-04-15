package com.inn.garg

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr


object WordCountStreaming extends Serializable {
  @transient lazy val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("starting word count streaming")
    val spark = SparkSession.builder()
                            .appName("Word Count")
                            .master("local[4]")
                            .config("spark.streaming.stopGracefullyOnShutdown", "true")
                            .config("spark.shuffle.partition", "20")
                            .getOrCreate()

    val df = spark.readStream
                  .format("socket")
                  .option("host", "localhost")
                  .option("port", "9999")
                  .load()
    val df1 = df.select(expr("explode(split(value,' ')) as value"))
    val df2 = df1.groupBy("value").count()
    val df3 = df2.writeStream
                 .format("console")
                 .option("checkpointLocation", "checkpoint2")
                 .outputMode("complete")
                 .start()
    df3.awaitTermination()
  }
}
