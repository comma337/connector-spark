package kafka.connector.spark.batch.writers

import kafka.connector.spark.config.KafkaSinkElement
import kafka.connector.spark.helpers.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaWriter {

  def apply(spark: SparkSession, conf: KafkaSinkElement): KafkaWriter = {
    if (conf.servers.isEmpty || conf.topic.isEmpty) {
      throw new IllegalArgumentException("Missing bootstrap server or topic value")
    }
    new KafkaWriter(spark, conf.servers, conf.topic, conf.format, conf.separator, conf.properties)
  }

}

class KafkaWriter(spark: SparkSession, servers: String, topic: String, format: String, separator: String, properties: Map[String, String])
  extends DataWriter(spark) with Logging {

  override def write(df: DataFrame): Unit = {
    log.trace(df.count())
    convertFormat(df)
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", servers)
      .option("topic", topic)
      .options(properties)
      .save()
  }

  private def convertFormat(df: DataFrame): DataFrame = {
    format match {
      case "csv" => convertCsv(df)
      case _ => throw new RuntimeException("No valid source")
    }
  }

  private def convertCsv(df: DataFrame): DataFrame = {
    df.select(concat_ws(separator, df.columns.map(c => col(c)): _*).as("value"))
  }

}
