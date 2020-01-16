package kafka.connector.spark.helpers

import java.time.{Duration, LocalDate, LocalDateTime}

object DateOperations extends Logging {

  def getDateList(interval: String, from: LocalDateTime, to: LocalDateTime): Seq[LocalDateTime] = {
    interval match {
      case "hour" => getHourlyDateList(from, to)
      case "day" => getDailyDateList(from, to)
      case _ => throw new RuntimeException("No valid interval")
    }
  }

  def getHourlyDateList(from: LocalDateTime, to: LocalDateTime): Seq[LocalDateTime] = {
    (0 to getHours(from, to)).map { i =>
      from.plusHours(i)
    }
  }

  def getDailyDateList(from: LocalDateTime, to: LocalDateTime): Seq[LocalDateTime] = {
    (0 to getDays(from, to)).map { i =>
      from.plusDays(i)
    }
  }

  def getHours(from: LocalDateTime, to: LocalDateTime): Int = {
    Duration.between(from, to).toHours.toInt
  }

  def getDays(from: LocalDateTime, to: LocalDateTime): Int = {
    getDays(from.toLocalDate, to.toLocalDate)
  }

  def getDays(from: LocalDate, to: LocalDate): Int = {
    Duration.between(from.atStartOfDay(), to.atStartOfDay()).toDays.toInt
  }

  def getWeeks(from: LocalDate, to: LocalDate): Int = {
    Duration.between(from.atStartOfDay(), to.atStartOfDay()).toDays.toInt / 7
  }

}
