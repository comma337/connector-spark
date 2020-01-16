package kafka.connector.spark.batch

import java.time.LocalDateTime

import kafka.connector.spark.batch.readers.{DataReader, HiveReader}
import kafka.connector.spark.batch.writers.{DataWriter, KafkaWriter}
import kafka.connector.spark.config.{AppConfig, LoadConfiguration, SparkAppElement}
import kafka.connector.spark.helpers.{DateOperations, Logging}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object BatchProcessor extends App with Logging {

  val conf = AppConfig(LoadConfiguration())
  val spark = getSparkSession(conf.spark)

  val reader: DataReader = getReader(spark, conf.batch.source, conf)
  val writer: DataWriter = getWriter(spark, conf.batch.sink, conf)
  val range: Seq[LocalDateTime] = DateOperations.getDateList(conf.batch.interval, conf.batch.from, conf.batch.to)

  range.foreach { t =>
    val df = reader.read(t)
    writer.write(df)
  }


  private def getWriter(spark:SparkSession, sink: String, config: AppConfig): DataWriter = {
    sink match {
      case "kafka" => KafkaWriter(spark, conf.sink_kafka)
      case _ => throw new RuntimeException("No valid sink")
    }
  }

  private def getReader(spark:SparkSession, source: String, config: AppConfig): DataReader = {
    source match {
      case "hive" => HiveReader(spark, conf.source_hive)
      case _ => throw new RuntimeException("No valid source")
    }
  }

  private def getSparkSession(sparkAppConf: SparkAppElement): SparkSession = {
    val sparkConfig = new SparkConf().setAll(sparkAppConf.properties)

    var sessionBuilder = SparkSession.builder()
    if(sparkAppConf.name.nonEmpty)
      sessionBuilder = sessionBuilder.appName(sparkAppConf.name)

    if(sparkAppConf.master.nonEmpty)
      sessionBuilder = sessionBuilder.master(sparkAppConf.master)

    if(sparkAppConf.enableHiveSupport)
      sessionBuilder = sessionBuilder.enableHiveSupport()

    sessionBuilder.config(sparkConfig).getOrCreate
  }

}
