name := "MemSQLRelation";
version := "0.1.2";
scalaVersion := "2.10.4";
libraryDependencies  ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.3.0" % "provided",
    "org.apache.spark" %% "spark-sql"  % "1.3.0" % "provided",
    "mysql" % "mysql-connector-java" % "5.1.34",
    "com.github.tototoshi" %% "scala-csv" % "1.2.1"
)
