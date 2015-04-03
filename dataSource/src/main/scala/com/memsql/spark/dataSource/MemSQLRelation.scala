package com.memsql.spark.dataSource

import java.sql.{DriverManager, ResultSet, Connection}

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import com.memsql.spark.dataSource.Write.MemSQLWriteDetails

// From ddl.scala in spark tree, with access modifier removed
class CaseInsensitiveMap(map: Map[String, String]) extends Map[String, String]
  with Serializable {

  val baseMap = map.map(kv => kv.copy(_1 = kv._1.toLowerCase))

  override def get(k: String): Option[String] = baseMap.get(k.toLowerCase)

  override def + [B1 >: String](kv: (String, B1)): Map[String, B1] =
    baseMap + kv.copy(_1 = kv._1.toLowerCase)

  override def iterator: Iterator[(String, String)] = baseMap.iterator

  override def -(key: String): Map[String, String] = baseMap - key.toLowerCase
}

class MemSQLDataFrame(val df: DataFrame) {
  def saveToMemSQL(url: String, table: String, scratchDir: String): Unit = {
    MemSQLWriteDetails.saveTable(df, url, table, scratchDir)
  }
}

class DefaultSource extends RelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val host = parameters.getOrElse("host", sys.error("Option 'host' not specified"))
    val port = parameters.getOrElse("port", sys.error("Option 'port' not specified"))
    val user = parameters.getOrElse("user", sys.error("Option 'user' not specified"))
    val password = parameters.getOrElse("password", "")
    val dbName = parameters.getOrElse("dbName", sys.error("Option 'dbName' not specified"))
    val dbtable = parameters.getOrElse("dbtable", sys.error("Option 'dbtable' not specified"))

    var url = s"jdbc:mysql://$host:$port/$dbName?user=$user"
    if (password != "") {
      url += s"&password=$password"
    }
    val conn = DriverManager.getConnection(url)
    val numPartitions = getNumPartitions(conn, dbName)

    var newParams = parameters
    if (isTableDistributed(conn, dbName, dbtable)) {
      newParams = new CaseInsensitiveMap(parameters + (
        "url" -> url,
        "lowerBound" -> "0",
        "numPartitions" -> s"${numPartitions}",
        "upperBound" -> s"${numPartitions}",
        "partitionColumn" -> "partition_id()"))
    }
    else {
      newParams = new CaseInsensitiveMap(parameters + (
        "url" -> url))
    }
    (new JDBCSource).createRelation(sqlContext, newParams)
  }

  def getNumPartitions(conn: Connection, database: String): Integer = {
    val q = s"SHOW PARTITIONS ON $database"
    var stmt = conn.createStatement
    val rs = stmt.executeQuery(q)
    var ret = -1
    try {
      while (rs.next()) {
        ret = (if (ret > rs.getInt("ordinal")) ret else rs.getInt("ordinal"))
      }
    }
    finally {
      stmt.close()
    }
    ret
  }

  def isTableDistributed(conn: Connection, database: String, table: String): Boolean = {
    val q = s"USING $database SHOW TABLES EXTENDED LIKE '$table'"
    var stmt = conn.createStatement
    val rs = stmt.executeQuery(q)
    rs.next()
    val ret = 1 == rs.getInt("distributed")
    stmt.close()
    ret
  }
}
