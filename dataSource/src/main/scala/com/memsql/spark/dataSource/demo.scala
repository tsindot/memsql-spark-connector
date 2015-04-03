import com.memsql.spark.dataSource
import com.memsql.spark.dataSource._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import java.sql.{DriverManager, ResultSet, Connection}

object MemSQLRelationUsageDemo {
  def main(args: Array[String]) {
     // Define our connection parameters for MemSQL.
     val host = "127.0.0.1"
     val port = 3306
     val user = "root"
     val password = ""
     val dbName = "memsql_relation_test_db"

     // Setup the schema in MemSQL, independent of Spark.
     {
       // We initially create two tables and populate them with direct
       // sql queries.
       val url = s"jdbc:mysql://$host:$port?user=$user"
       val conn = DriverManager.getConnection(url)
       var stmt = conn.createStatement
       stmt.execute(s"DROP DATABASE IF EXISTS $dbName")
       stmt.execute(s"CREATE DATABASE $dbName")
       stmt.execute(s"USE $dbName")
       stmt.execute("""
          CREATE TABLE users
          (user_id INTEGER PRIMARY KEY, name VARCHAR(200))
       """)
       stmt.execute("""
          CREATE REFERENCE TABLE groups
          (group_id INTEGER PRIMARY KEY, group_name VARCHAR(200), abbr as SUBSTRING(group_name, 0, 5) persisted varchar(5))
       """)
       stmt.execute("""
          CREATE TABLE users_groups
          (user_id INTEGER NOT NULL, group_id INTEGER NOT NULL, KEY (user_id, group_id), KEY (group_id, user_id), SHARD KEY (user_id))
       """)
     }

     // Create our Spark and SQL context.
     val conf = new SparkConf().setAppName("MemSQLRelation Usage Demo")
     val sc = new SparkContext(conf)
     val sqlContext = new SQLContext(sc)

     // Create a couple dataframes to start with. See "Programmatically Specifying the Schema"
     // on https://spark.apache.org/docs/1.3.0/sql-programming-guide.html
     // for details.
     val source_users_df = {
       val usersRDD = sc.parallelize(List.range(0, 1000)).map(i => Row(i, s"person$i"))
       val schema = StructType(List(
         StructField("user_id", IntegerType, true),
         StructField("name", StringType, true)))
       sqlContext.createDataFrame(usersRDD, schema)
     }
     // Note that we elide the computed column, which MemSQL will fill in.
     val source_groups_df = {
       val groupsRDD = sc.parallelize(List.range(0, 100)).map(i => Row(i, s"group$i"))
       val schema = StructType(List(
         StructField("group_id", IntegerType, true),
         StructField("group_name", StringType, true)))
       sqlContext.createDataFrame(groupsRDD, schema)
     }

     // We can save these dataframes to MemSQL.
     val url = s"jdbc:mysql://$host:$port/$dbName?user=$user"
     source_users_df.saveToMemSQL(url, "users", "/tmp")
     source_groups_df.select("group_id", "group_name").saveToMemSQL(url, "groups", "/tmp")

     // Now we can retrieve values from the MemSQL tables.
     val users_df = sqlContext.load("com.memsql.spark.dataSource", Map(
       "host" -> host,
       "port" -> s"$port",
       "user" -> user,
       "password" -> password,
       "dbName" -> dbName,
       "dbtable" -> "users"))
     val groups_df = sqlContext.load("com.memsql.spark.dataSource", Map(
       "host" -> host,
       "port" -> s"$port",
       "user" -> user,
       "password" -> password,
       "dbName" -> dbName,
       "dbtable" -> "groups"))

     // We can do a join, and save the results in a MemSQL table. Note
     // that only scans and filters are pushed down to MemSQL.
     val join_df = users_df.join(groups_df, users_df("user_id") === groups_df("group_id")).filter(groups_df("group_id") > 12)
       .select(users_df("user_id"), groups_df("group_id"))

     join_df.saveToMemSQL(url, "users_groups", "/tmp")

     val users_groups_df = sqlContext.load("com.memsql.spark.dataSource", Map(
       "host" -> host,
       "port" -> s"$port",
       "user" -> user,
       "password" -> password,
       "dbName" -> dbName,
       "dbtable" -> "users_groups"))

     var cnt = source_users_df.select("user_id", "user_id").filter(source_users_df("user_id") > 10).count()
     println(s"${cnt}")
     cnt = users_df.select("user_id", "user_id").filter(users_df("user_id") > 10).count()
     println(s"${cnt}")
     cnt = users_groups_df.select("user_id", "group_id").filter(users_groups_df("user_id") > 10).count()
     println(s"${cnt}")
  }
}
