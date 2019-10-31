package com

import org.apache.avro.Schema
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.Properties
import java.io.File

import org.apache.avro.file.DataFileReader

import scala.collection.JavaConverters
import scala.collection.JavaConversions

object Producer extends App {
  val schemaString = scala.io.Source.fromFile("avro/userdata.avsc").mkString
  val schema = (new Schema.Parser).parse(schemaString)
  val injection = GenericAvroCodecs.toBinary[GenericRecord](schema)
  val propMap = Map(
    "bootstrap.servers" -> "b-3.test-cluster2.wngukg.c2.kafka.ap-south-1.amazonaws.com:9092,b-1.test-cluster2.wngukg.c2.kafka.ap-south-1.amazonaws.com:9092,b-2.test-cluster2.wngukg.c2.kafka.ap-south-1.amazonaws.com:9092",
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
  propMap.foreach(x => props.put(x._1, x._2))
    props.entrySet().toArray().foreach(println(_))
  val producer = new KafkaProducer[String, Array[Byte]](props)
  var i =1
  val fileReader = new DataFileReader[GenericRecord](new File("avro/userdata1.avro"), new GenericDatumReader[GenericRecord](schema))
  while(fileReader.hasNext) {
    val x = fileReader.next
    val bytes = injection.apply(x)
//    println(i)
    i=i+1
    val record = new ProducerRecord[String,Array[Byte]]("test",bytes)
    println(record.value())
    producer.send(record)
  }
  producer.flush()
  producer.close()
}
