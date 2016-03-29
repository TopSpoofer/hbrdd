name := "hbrdd"

version := "1.0"

scalaVersion := "2.10.5"

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-language:postfixOps",
  "-language:implicitConversions",
  "-language:reflectiveCalls"
)

org.scalastyle.sbt.ScalastylePlugin.Settings

resolvers ++= Seq(
  "Cloudera repos" at "https://repository.cloudera.com/artifactory/cloudera-repos",
  "Cloudera releases" at "https://repository.cloudera.com/artifactory/libs-release"
)

val sparkVersion = "1.5.0"
val hbaseVersion = "1.0.0-cdh5.5.1"
val hadoopVersion = "2.6.0-cdh5.5.1"
val lang3Version = "3.0"
val jacksonVersion = "3.2.11"

val hadoopCommon = "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "compile" excludeAll ExclusionRule(organization = "javax.servlet")
val hadoopHdfs = "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion

val hbase = "org.apache.hbase" % "hbase" % hbaseVersion
val hbaseCommon = "org.apache.hbase" % "hbase-common" % hbaseVersion % "compile"
val hbaseClient = "org.apache.hbase" % "hbase-client" % hbaseVersion % "compile"
val hbaseServer = "org.apache.hbase" % "hbase-server" % hbaseVersion % "compile" excludeAll ExclusionRule(organization = "org.mortbay.jetty")
val hbaseProtocol = "org.apache.hbase" % "hbase-protocol" % hbaseVersion % "compile"
val hbaseHadoopCompat = "org.apache.hbase" % "hbase-hadoop-compat" % hbaseVersion % "compile"
val hbaseHadoop2Compat = "org.apache.hbase" % "hbase-hadoop2-compat" % hbaseVersion % "compile"

//val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion % "compile"
//val sparkStreaming = "org.apache.spark" %% "spark-streaming" % sparkVersion % "compile"

val lang3 = "org.apache.commons" % "commons-lang3" % lang3Version
val jackson = "org.json4s" %% "json4s-jackson" % jacksonVersion % "provided"

libraryDependencies ++= Seq(
  hadoopCommon,
  hadoopHdfs,
  hbase,
  hbaseCommon,
  hbaseClient,
  hbaseProtocol,
  hbaseHadoopCompat,
  hbaseHadoop2Compat,
  hbaseServer,
//  sparkCore,
//  sparkStreaming,
  lang3,
  jackson
)

fork in Test := true