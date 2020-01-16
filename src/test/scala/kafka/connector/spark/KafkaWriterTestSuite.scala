package kafka.connector.spark

import kafka.connector.spark.batch.writers.KafkaWriter
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class KafkaWriterTestSuite extends AnyFunSuite {

  private def getSparkSession(): SparkSession = {
    SparkSession.builder()
      .master("local[1]")
      .getOrCreate()
  }

  test("send data to kafka") {
    val spark = getSparkSession
    val data = Seq((1, "aa"), (2, "bb"), (3, "cc"), (4, "dd"), (5, "ee"))
    val df = spark.createDataFrame(data)
    val writer = new KafkaWriter(spark, "10.205.72.189:9092", "test", "csv", ",", Map())
    writer.write(df)
  }

  test("read data from kafka") {
    val df = getSparkSession.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.205.72.189:9092")
      .option("subscribe", "test")
      .option("group.id", "consumer-test")
      .load()
    df.show()
  }

}
