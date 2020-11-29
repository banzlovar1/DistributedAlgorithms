import sbt.Keys.{libraryDependencies, scalaVersion, version}


lazy val root = (project in file(".")).
  settings(
    name := "CPTS-415-Project",

    version := "0.1.0",

    scalaVersion := "2.12.8",

    organization  := "org.dataoceanlab",

    publishMavenStyle := true,

    mainClass := Some("cpts415.driver"),

    resolvers += "SparkPackages" at "https://dl.bintray.com/spark-packages/maven/"
  )

logLevel := Level.Warn

logLevel in assembly := Level.Error

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.1" % "compile",
  "org.apache.spark" %% "spark-sql" % "3.0.1" % "compile",
  "org.apache.spark" %% "spark-graphx" % "3.0.1" % "compile",
  "graphframes" % "graphframes" % "0.8.1-spark3.0-s_2.12" % "compile",
  "org.scalatest" %% "scalatest" % "3.1.4" % "test"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case path if path.endsWith(".SF") => MergeStrategy.discard
  case path if path.endsWith(".DSA") => MergeStrategy.discard
  case path if path.endsWith(".RSA") => MergeStrategy.discard
  case _ => MergeStrategy.first
}