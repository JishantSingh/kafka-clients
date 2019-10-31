package com

import java.io.File
import java.util.Properties

import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object LocalProducer extends App {
  val schemaString = scala.io.Source.fromFile("src/main/resources/avro/userdata.avsc").mkString
  val schema = (new Schema.Parser).parse(schemaString)
  val injection = GenericAvroCodecs.toBinary[GenericRecord](schema)
  val propMap = Map(
    "bootstrap.servers" -> "b-1.test-cluster.zeebgn.c2.kafka.ap-south-1.amazonaws.com:9094,b-2.test-cluster.zeebgn.c2.kafka.ap-south-1.amazonaws.com:9094",
    "value.serializer" -> "org.apache.kafka.common.serialization.ByteArraySerializer",
    "acks" -> "all",
    "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
  )
  val propMap1 = Map(
    "bootstrap.servers" -> "localhost:9092",
    "value.serializer" -> "org.apache.kafka.common.serialization.ByteArraySerializer",
    "acks" -> "all",
    "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
  )
  val propMap2 = Map(
    "bootstrap.servers" -> "localhost:9092",
    "value.serializer" -> "org.apache.kafka.common.serialization.ByteArraySerializer",
    "acks" -> "all",
    "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
  )
  val props = new Properties()
  propMap1.foreach(x => props.put(x._1, x._2))
    props.entrySet().toArray().foreach(println(_))
  val producer = new KafkaProducer[String, Array[Byte]](props)
  var i =1
  val fileReader = new DataFileReader[GenericRecord](new File("src/main/resources/avro/userdata1.avro"), new GenericDatumReader[GenericRecord](schema))
  while(fileReader.hasNext) {
    val x = fileReader.next
    val bytes = injection.apply(x)
    i=i+1
    val record = new ProducerRecord[String,Array[Byte]]("test",bytes)
    println(record.value())
    producer.send(record)
  }
  producer.flush()
  producer.close()
}
