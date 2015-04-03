package com.memsql.spark

import org.apache.spark.sql._

package object dataSource {
  implicit def toMemSQLDataFrame(df: DataFrame) = new MemSQLDataFrame(df)
}
