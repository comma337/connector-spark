package kafka.connector.spark.batch.writers

import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class DataWriter(spark: SparkSession) {

  def write(df: DataFrame)

}
