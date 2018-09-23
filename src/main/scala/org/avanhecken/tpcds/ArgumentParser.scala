package scala.org.avanhecken.tpcds

object ArgumentParser {
  type Args = Map[String, String]

  def parse(args: Array[String]): Args = {
    args match {
      case Array(name, description, db, ids) => Map("name" -> name, "description" -> description, "db" -> db, "ids" -> ids)
      case Array(name, description, db) => Map("name" -> name, "description" -> description, "db" -> db)
      case _ => throw new RuntimeException(
        """Invalid arguments!
          |  Required (in this order): name, description, database
          |  Optional: list of ids""".stripMargin)
    }
  }
}
