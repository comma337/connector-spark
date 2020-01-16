package kafka.connector.spark.batch.readers

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import kafka.connector.spark.config.HiveSourceElement
import kafka.connector.spark.helpers.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveReader {

  def apply(spark: SparkSession, conf: HiveSourceElement): HiveReader = {
    if (conf.table.isEmpty || conf.partition_pattern.isEmpty) {
      throw new IllegalArgumentException("Missing table or partition value")
    }
    new HiveReader(spark, conf.table, conf.partition_pattern)
  }

}

class HiveReader(spark: SparkSession, table: String, partitionPattern: String) extends DataReader(spark) with Logging {

  override def read(targetDate: LocalDateTime): DataFrame = {
    val partition = targetDate.format(DateTimeFormatter.ofPattern(partitionPattern))
    spark.table(table).filter(partition)
  }

}
