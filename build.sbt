// Build file for bulding spark scala code in folder src
name := "SparkDev"
version := "1.0"
scalaVersion := "2.11.8"

val sparkVersion = "2.0.0"

// Base Spark-provided dependencies
libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % sparkVersion,
	"org.apache.spark" %% "spark-streaming" % sparkVersion,
	"org.apache.spark" %% "spark-sql" % sparkVersion,
	"org.apache.spark" %% "spark-graphx" % sparkVersion)

