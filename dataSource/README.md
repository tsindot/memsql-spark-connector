This package provides an implementation of the Spark Data Sources API
for MemSQL. For example usage, see
src/main/scala/com/memsql/spark/dataSource/demo.scala

Compile the package with `sbt package`. Note `sbt package` produces a
standalone jar which does not contain dependencies. Run `sbt assembly`
to produce a fat jar which includes transitive dependencies.

Running the demo app can be done by tweaking paths in the run.sh shell
script and executing it.

The files JDBCRDD.scala, JDBCRelation.scala, jdbc.scala and DriverQuirks.scala
are sourced from version 1.3.0 of the main Spark repository. They have been
added to the com.memsql.spark.dataSource package and the private
access modifiers have been stripped. Other than that only minor
changes have been made, except for jdbc.scala (which now uses LOAD
DATA in place of singleton inserts)

To use in client code, use
  import com.memsql.spark.dataSource
  import com.memsql.spark.dataSource._