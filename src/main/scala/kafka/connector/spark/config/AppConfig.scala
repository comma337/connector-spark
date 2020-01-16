package kafka.connector.spark.config

import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.ChronoField

import com.typesafe.config.Config

import scala.collection.JavaConverters._

case class AppConfig(conf: Config) {
  val spark = new SparkAppElement(conf.getConfig("spark"))
  val batch = new BatchElement(conf.getConfig("batch"))
  val source_hive = new HiveSourceElement(conf.getConfig("source_hive"))
  val sink_kafka = new KafkaSinkElement(conf.getConfig("sink_kafka"))
}

class SparkAppElement(val conf: Config) {
  val name: String = conf.getString("name")
  val master: String = conf.getString("master")
  val enableHiveSupport: Boolean = conf.getBoolean("enableHiveSupport")
  val properties: Predef.Map[String, String] = ConfigHelper.convertToMap(conf.getConfig("properties"))
}

class BatchElement(val conf: Config) {
  val interval: String = conf.getString("interval")
  val source: String = conf.getString("source")
  val sink: String = conf.getString("sink")

  private val date_pattern: DateTimeFormatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd[ [HH][:mm][:ss][.SSS]]")
    .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
    .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
    .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
    .toFormatter()
  val from: LocalDateTime = LocalDateTime.parse(conf.getString("from"), date_pattern)
  val to: LocalDateTime = LocalDateTime.parse(conf.getString("to"), date_pattern)
}

class HiveSourceElement(val conf: Config) {
  val table: String = conf.getString("table")
  val partition_pattern: String = conf.getString("partition_pattern")
}

class KafkaSinkElement(val conf: Config) {
  val servers: String = conf.getStringList("servers").asScala.mkString(",")
  val topic: String = conf.getString("topic")
  val format: String = conf.getString("format")
  val separator: String = conf.getString("separator")
  val properties: Predef.Map[String, String] = ConfigHelper.convertToMap(conf.getConfig("properties"))
}

object ConfigHelper {
  def convertToMap(conf: Config): Predef.Map[String, String] = {
    conf.entrySet().asScala.map(x => (x.getKey, x.getValue.unwrapped().toString)).toMap
  }
}