ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.8"

lazy val root = (project in file("."))
  .settings(
    name := "anomalydetection"
  )
resolvers += "cloudera repo" at "https://repository.cloudera.com/artifactory/libs-release-local/"
libraryDependencies ++= Seq(
  //HADOOP DEPENDENCIES
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "org.apache.spark" %% "spark-sql" % "2.4.8" % "provided",
  "org.apache.spark" %% "spark-hive" % "2.4.8" % "compile",
  "org.apache.spark" %% "spark-yarn" % "2.4.8" % "provided",
  "org.apache.spark" %% "spark-mllib" % "2.4.8" % "provided",
  //DATA QUALITY DEPENDENCIES
  "com.amazon.deequ" % "deequ" % "1.1.0_spark-2.4-scala-2.11"
)
// https://mvnrepository.com/artifact/org.mongodb.spark/mongo-spark-connector
libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.4"