package com

import java.io.File
import java.util.Properties

import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object StringProducer extends App {
  val propMap = Map(
    "bootstrap.servers" -> "b-3.test-cluster2.wngukg.c2.kafka.ap-south-1.amazonaws.com:9092,b-1.test-cluster2.wngukg.c2.kafka.ap-south-1.amazonaws.com:9092,b-2.test-cluster2.wngukg.c2.kafka.ap-south-1.amazonaws.com:9092",
    "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "acks" -> "all",
    "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
  )
  val propMap1 = Map(
    "bootstrap.servers" -> "localhost:9092",
    "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
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
  propMap.foreach(x => props.put(x._1, x._2))
    props.entrySet().toArray().foreach(println(_))
  val producer = new KafkaProducer[String, String](props)
  var i =1
  (1 to 10).toArray.foreach(x => println((producer.send(new ProducerRecord[String,String]("test",s"key_$x", s"value_$x"))).get()))

  producer.flush()
  producer.close()
}
