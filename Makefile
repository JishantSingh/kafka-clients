build: 
	sbt clean
	sbt assembly

producer:
	java -cp target/scala-2.12/kafka-clients-assembly-0.1.jar com.Producer

consumer:
	java -cp target/scala-2.12/kafka-clients-assembly-0.1.jar com.Consumer
