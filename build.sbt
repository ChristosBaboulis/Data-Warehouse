name := "MyProject"

version := "0.1.0"

scalaVersion := "2.12.18"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.0",
  "org.apache.spark" %% "spark-sql" % "3.4.0"
)
