This package provides an implementation of the Spark Data Sources API
for MemSQL. For example usage, see
src/main/scala/com/memsql/spark/dataSource/demo.scala

To produce a jar that contains transitive dependencies, run `sbt assembly`. The
resulting jar will be located at target/scala-2.10/MemSQLRelation-assembly-0.1.2.jar.
To exclude transitive dependencies, instead use `sbt package` and the resulting
file target/scala-2.10/memsqlrelation_2.10-0.1.2.jar  

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
