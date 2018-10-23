package org.avanhecken.tpcds

import com.typesafe.scalalogging.LazyLogging

object ArgumentParser extends LazyLogging {
  type Args = Map[String, String]

  def parse(args: Array[String]): Args = {
    val parsedArgs = args match {
      case Array(command, database) if command == "list" =>
        Map("command" -> command, "database" -> database)
      case Array(command, name, description, database, resourceLocation) if command == "execute" =>
        Map("command" -> command, "name" -> name, "description" -> description, "database" -> database, "resource_location" -> resourceLocation)
      case Array(command, name, description, database, resourceLocation, ids) if command == "execute" =>
        Map("command" -> command, "name" -> name, "description" -> description, "database" -> database, "resource_location" -> resourceLocation, "ids" -> ids)
      case Array(command, database, name1, name2) if command == "compare" =>
        Map("command" -> command, "database" -> database, "name1" -> name1, "name2" -> name2)
      case _ => throw new RuntimeException(
        """Invalid arguments!
          |  Possible commands are: list, execute and compare
          |
          |  Commands:
          |    * list database
          |    * execute name description database resource_location [ids]
          |    * compare database name1 name2
          |  """.stripMargin)
    }

    logger.trace(parsedArgs.mkString(" | "))

    parsedArgs
  }
}
