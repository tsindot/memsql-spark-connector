/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.memsql.spark.dataSource

import util.Random.nextInt

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.io.File

import org.apache.spark.{Logging, Partition}
import org.apache.spark.sql._
import org.apache.spark.sql.sources.LogicalRelation
import org.apache.spark.sql.types._

import com.github.tototoshi.csv._

package object Write {

  implicit object Format extends DefaultCSVFormat {
    override val delimiter = '\t'
  }

  object MemSQLWriteDetails extends Logging {
    def loadDataQuery(conn: Connection, table: String, rddSchema: StructType, tempFile: File):
        String = {
      val sql = new StringBuilder(s"LOAD DATA LOCAL INFILE '$tempFile' INTO TABLE $table (")

      // We need to not insert into computed columns
      // We could also just not run this check - in that case we'll
      // just bubble up an exception from MemSQL
      val stmt = conn.createStatement
      try {
        val rs = stmt.executeQuery(s"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '$table'
          AND extra LIKE '%computed%'
        """)
        var computed_columns = Set(): Set[String]
        while (rs.next()) {
          computed_columns += rs.getString("column_name")
        }
        for (field <- rddSchema.fields) {
          if (computed_columns contains field.name) {
            throw new IllegalArgumentException(s"Cannot insert into computed column '${field.name}'")
          }
        }

        var addComma = false
        for (field <- rddSchema.fields) {
          if (addComma) {
            sql.append(", ")
          }
          sql.append(field.name)
          addComma = true
        }
        sql.append(")")
        sql.toString
      }
      finally {
        stmt.close()
      }
    }

    def savePartitionWithLoadData(
        url: String, table: String, iterator: Iterator[Row],
        rddSchema: StructType, nullTypes: Array[Int], scratchDir: String): Unit = {
      val conn = DriverManager.getConnection(url)
      val tempFile = new File(s"${scratchDir}/memsql-relation-load-${nextInt}")
      try {
        val writer = CSVWriter.open(tempFile)
        try {
          while (iterator.hasNext) {
            val row = iterator.next()
            writer.writeRow(row.toSeq)
          }
        }
        finally {
          writer.close()
        }

        val q = loadDataQuery(conn, table, rddSchema, tempFile)
        conn.setAutoCommit(false)
        val stmt = conn.createStatement
        try {
          stmt.executeQuery(q)
        }
        finally {
          stmt.close()
        }
        conn.commit()
      }
      finally {
        tempFile.delete()
        conn.close()
      }
    }

    /**
     * Saves the RDD to the database in a single transaction.
     */
    def saveTable(df: DataFrame, url: String, table: String, scratchDir: String) {
      val quirks = DriverQuirks.get(url)
      var nullTypes: Array[Int] = df.schema.fields.map(field => {
        var nullType: Option[Int] = quirks.getJDBCType(field.dataType)._2
        if (nullType.isEmpty) {
          field.dataType match {
            case IntegerType => java.sql.Types.INTEGER
            case LongType => java.sql.Types.BIGINT
            case DoubleType => java.sql.Types.DOUBLE
            case FloatType => java.sql.Types.REAL
            case ShortType => java.sql.Types.INTEGER
            case ByteType => java.sql.Types.INTEGER
            case BooleanType => java.sql.Types.BIT
            case StringType => java.sql.Types.CLOB
            case BinaryType => java.sql.Types.BLOB
            case TimestampType => java.sql.Types.TIMESTAMP
            case DateType => java.sql.Types.DATE
            case DecimalType.Unlimited => java.sql.Types.DECIMAL
            case _ => throw new IllegalArgumentException(
              s"Can't translate null value for field $field")
          }
        } else nullType.get
      }).toArray

      val rddSchema = df.schema
      df.foreachPartition { iterator =>
        MemSQLWriteDetails.savePartitionWithLoadData(url, table, iterator, rddSchema, nullTypes, scratchDir)
      }
    }
  }
}
