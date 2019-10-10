name := "xlz_data"

version := "0.1"

scalaVersion := "2.11.8"


resolvers ++= Seq(
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cdh-releases-rcs/",
  "Elasticsearch Repository" at "https://s3.amazonaws.com/download.elasticsearch.org/lucenesnapshots/83f9835"
)


libraryDependencies += "org.apache.spark"%"spark-core_2.11"%"2.2.0-cdh6.0.1"

libraryDependencies += "org.apache.spark"%"spark-sql_2.11"%"2.2.0-cdh6.0.1"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.0-cdh6.0.1"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.2.0-cdh6.0.1"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.2.0-cdh6.0.1"

libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "2.2.0-cdh6.0.1"

libraryDependencies += "org.apache.hbase" % "hbase-client" % "2.0.0-cdh6.0.1"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "2.0.0-cdh6.0.1"

libraryDependencies += "org.apache.hbase" % "hbase-server" % "2.0.0-cdh6.0.1"

libraryDependencies += "org.apache.hbase" % "hbase-mapreduce" % "2.0.0-cdh6.0.1"

libraryDependencies += "org.apache.hbase" % "hbase-spark" % "2.0.0-cdh6.0.1"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.6"
// https://mvnrepository.com/artifact/org.apache.hbase/hbase
libraryDependencies += "org.apache.hbase" % "hbase" % "2.0.0-cdh6.0.1" pomOnly()
// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.0.0-cdh6.0.1"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.6.5"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5"

dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.6.5"

libraryDependencies += "net.sf.json-lib" % "json-lib" % "2.3" from "http://repo1.maven.org/maven2/net/sf/json-lib/json-lib/2.3/json-lib-2.3-jdk15.jar"

libraryDependencies += "org.elasticsearch.client" % "elasticsearch-rest-high-level-client" % "7.2.0"

libraryDependencies += "org.elasticsearch" % "elasticsearch" % "7.2.0"

libraryDependencies += "junit" % "junit" % "4.12" % Test

libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.12.0"

// https://mvnrepository.com/artifact/log4j/log4j
libraryDependencies += "log4j" % "log4j" % "1.2.17"
