package org.avanhecken.tpcds

import ch.qos.logback.classic.{Level, Logger}
import org.slf4j.LoggerFactory

/** @TODO Next step, use configuration file */
trait Logging {
  /** Silence all none-application loggers (like spark etc.) */
  private val rootLogger = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger]
  rootLogger.setLevel(Level.ERROR)

  /** Set the logging of the application to info */
  private val appLogger = LoggerFactory.getLogger("org.avanhecken.tpcds").asInstanceOf[Logger]
  appLogger.setLevel(Level.INFO)
}
