package com.memsql.spark.connector.util

import java.sql.{Connection, DriverManager}

object MemSQLDriverManager {
  val DEFAULT_JDBC_LOGIN_TIMEOUT = 10 //seconds

  def getConnection(dbAddress: String, user: String, password: String): Connection = {
    DriverManager.setLoginTimeout(DEFAULT_JDBC_LOGIN_TIMEOUT)
    DriverManager.getConnection(dbAddress, user, password)
  }
}
