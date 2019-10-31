name := "kafka-clients"

version := "0.1"

scalaVersion := "2.12.1"

libraryDependencies ++= Seq("org.apache.kafka" % "kafka-clients" % "2.2.1",
  "com.twitter" %% "bijection-avro" % "0.9.6",
  "org.apache.avro" % "avro" % "1.9.1"
)

mainClass in assembly := Some("com.Producer")