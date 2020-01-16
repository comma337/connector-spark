package kafka.connector.spark.batch.readers

import java.time.LocalDateTime

import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class DataReader(spark: SparkSession) {

  def read(targetDate: LocalDateTime): DataFrame

}
