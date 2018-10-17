package org.avanhecken.tpcds

object ArgumentParser {
  type Args = Map[String, String]

  val parsedArgs: Args = Map()

  def parse(args: Array[String]): Args = {
    args match {
      case Array(command, database) if command == "list" =>
        parsedArgs + ("command" -> command, "database" -> database)
      case Array(command, name, description, database, resourceLocation) if command == "execute" =>
        parsedArgs + ("command" -> command, "name" -> name, "description" -> description, "database" -> database, "resource_location" -> resourceLocation)
      case Array(command, name, description, database, resourceLocation, ids) if command == "execute" =>
        parsedArgs + ("command" -> command, "name" -> name, "description" -> description, "database" -> database, "resource_location" -> resourceLocation, "ids" -> ids)
      case Array(command, database, name1, name2) if command == "compare" =>
        parsedArgs + ("command" -> command, "database" -> database, "name1" -> name1, "name2" -> name2)
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
  }
}
