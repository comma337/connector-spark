package kafka.connector.spark

import java.time.LocalDateTime

import com.typesafe.config.{Config, ConfigFactory}
import kafka.connector.spark.batch.readers.HiveReader
import kafka.connector.spark.config.HiveSourceElement
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

class HiveReaderTestSuite extends AnyFunSuite {

  private def getSparkSession(): SparkSession = {
    SparkSession.builder()
      .master("spark://7f3a14c8d5db:7077")
      .config("hive.metastore.uris", "thrift://127.0.0.1:9083")
      .enableHiveSupport().getOrCreate()
  }

  test("connect hive server") {
    val spark = getSparkSession
    val df = spark.sql("show databases")
    df.show()
  }

  test("create hive table") {
    val spark = getSparkSession
    spark.sql("CREATE DATABASE IF NOT EXISTS test LOCATION 'hdfs://namenode:8020/user/hive/warehouse/test.db'")
    spark.sql("DROP TABLE IF EXISTS test.demo")
    spark.sql("CREATE TABLE test.demo (id int, name string) PARTITIONED BY (year string, month string, day string) STORED AS TEXTFILE")
    spark.sql(
      """
        |INSERT INTO TABLE test.demo PARTITION (year='2020', month='01', day='01')
        |VALUES (1, 'aa'), (2, 'bb'), (3, 'cc'), (4, 'dd'), (5, 'ee')
        |""".stripMargin)
    spark.sql("select * from test.demo").show()
  }

  test("read hive data with HiveReader") {
    val date = LocalDateTime.of(2020, 1, 1, 0, 0, 0)
    val df: HiveReader = new HiveReader(getSparkSession, "test.demo", "'year =' ''yyyy'' 'and month =' ''MM'' 'and day =' ''dd'' ")
    df.read(date).show()
  }

}
