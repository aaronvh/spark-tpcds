package org.avanhecken.tpcds.query

import java.io.File
import org.avanhecken.tpcds.ArgumentParser.Args
import org.avanhecken.tpcds.statement.Statement
import scala.io.{Codec, Source}
import scala.collection.JavaConverters._

/**
  * Factory to create the 99 queries of the TPC-DS benchmark.
  */
class QueryFactory(args: Args) extends QueryConfig {
  override val resourceLocation = args("resource_location")

  /**
    * Generate a list of queries based on a list of ids passed on as argument but when not provided then return all queries.
    *
    * @param args
    * @return
    */
  def generateQueries(name: String): Array[Query] = {
    /** Statements found in the sql file. */
    def statements(name: String, queryId: String, resourceLocation: String): Array[Statement] = {
      val sqlFileLocation: File = new File(s"$resourceLocation/queries/$queryId.sql")

      implicit val codec = Codec("UTF-8")
      Source.fromFile(sqlFileLocation.getPath)
        .mkString
        .trim
        .split(";")
        .zipWithIndex
        .map {
          case (text, statementId) =>
            val id: String = s"$name.$queryId.$statementId"
            Statement(id, text)
        }
    }

    /** List of ids provided by command line. */
    val queryIds: Option[Set[String]] = args
      .get("ids")
      .map(_.split(",").toSet)

    /** All keys in the queries config file. */
    val keys = config
      .getObject("queries")
      .entrySet()
      .asScala
      .map(_.getKey)

    /** Map ids to queries.  If no ids are given then return all queries. */
    queryIds
      .map(ids => ids.intersect(keys))
      .getOrElse(keys)
      .map {
        key =>
          Query(
            key,
            getBusinessQuestion(key),
            getClass(key),
            statements(name, key, resourceLocation)
          )
      }.toArray
  }
}

