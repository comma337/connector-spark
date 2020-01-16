package kafka.connector.spark.helpers

import org.apache.logging.log4j.{LogManager, Logger}

trait Logging {

  protected lazy val log: Logger = LogManager.getLogger(getClass.getName)

}
