package kafka.connector.spark.config

import com.typesafe.config.{Config, ConfigFactory}
import kafka.connector.spark.helpers.Logging

/**
 * 실행 환경 별 config file 을 생성하고
 * 실행 시 환경변수 config.resource 에 root node 및 파일 경로를 설정하여 사용합니다.
 * use : -Dconfig.root=app -Dconfig.resource=config/local.conf
 */
object LoadConfiguration extends Logging {

  def apply(): Config = {
    val path = System.getProperty("config.resource", "local.conf")
    val root = System.getProperty("config.root", "app")
    log.trace(s"load config... file: $path, root: $root")
    ConfigFactory.load(path).getConfig(root)
  }

}
