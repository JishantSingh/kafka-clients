package com
import java.util.Properties
import java.io.File
import java.time.Duration

import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
//import scala.collection.jcl.MutableIterator.Wrapper
import collection.mutable._
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

object Consumer extends App {
  val propsMap = Map("bootstrap.servers"-> "b-3.test-cluster2.wngukg.c2.kafka.ap-south-1.amazonaws.com:9092,b-1.test-cluster2.wngukg.c2.kafka.ap-south-1.amazonaws.com:9092,b-2.test-cluster2.wngukg.c2.kafka.ap-south-1.amazonaws.com:9092",
    "group.id"-> "test",
    "key.deserializer"-> "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer"
  )
  val props = new Properties()
  propsMap.foreach(x => props.put(x._1, x._2))
  val consumer = new KafkaConsumer[String,Array[Byte]](props)
  consumer.subscribe(java.util.Arrays.asList("test"))
  val schemaString = scala.io.Source.fromFile("avro/userdata.avsc").mkString
  val schema = (new Schema.Parser).parse(schemaString)
  val injection = GenericAvroCodecs.toBinary[GenericRecord](schema)
  while(true){
    val records = consumer.poll(Duration.ofMillis(100))
    records.asScala.foreach(x=> println(injection.invert(x.value())))
  }
}
