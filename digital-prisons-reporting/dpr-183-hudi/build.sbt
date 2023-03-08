ThisBuild / version := "0.1.0-hudi-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "dpr-183-lake-house"
  )

val sparkVersion = "3.3.1"

libraryDependencies ++= Seq(
//  "org.apache.spark" % "spark-core_2.12" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.12" % sparkVersion,
  "com.amazonaws" % "aws-java-sdk-core" % "1.12.419",
  "com.amazonaws" % "amazon-kinesis-client" % "1.14.10",
  "com.nimbusds" % "nimbus-jose-jwt" % "9.30.2",
  "io.delta" %% "delta-core" % "2.2.0",
  "io.delta" %% "delta-contribs" % "2.2.0",
//  "org.apache.hudi" %% "hudi-spark-bundle" % "0.12.0",
  "org.apache.spark" %% "spark-avro" % "3.3.1",
  "org.apache.hudi" %% "hudi-utilities-bundle" % "0.12.2"
//   ("org.apache.hudi" %% "hudi-spark3" % "0.12.0").excludeAll(ExclusionRule("org.json4s", "json4s-jackson_2.11"))
//  "org.apache.hudi" %% "hudi-spark3.1.x" % "0.12.2"
//  "org.apache.hudi" % "hudi-spark3-common" % "0.12.2"
//  "org.apache.hudi" %% "hudi-spark3" % "0.11.1"
//    "org.apache.spark" %% "spark-streaming-kinesis-asl" % "3.3.1",
//  "com.hadoop.gplcompression" % "hadoop-lzo" % "0.4.16"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}