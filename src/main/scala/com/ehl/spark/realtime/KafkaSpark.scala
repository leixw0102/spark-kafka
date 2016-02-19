package com.ehl.spark.realtime

import com.ehl.spark.example.SparkStreamOp
import org.apache.spark.streaming.kafka.SparkKafka2ZkManager
import kafka.serializer.StringDecoder
import kafka.serializer.StringEncoder
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import java.util.Date

/**
 * @author ehl
 */
object KafkaSpark extends SparkStreamOp  with App {
  operateStreamSpark(args)(sc=>{

  val topicsSet = Set("page_visits");//.toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "host215:9092,host234:9092","group.id"->"spark-test")
    
    val skm=SparkKafka2ZkManager(kafkaParams);
    
    val line=skm.createKafkaDirectStream[String,String,StringDecoder, StringDecoder](sc, topicsSet);
//    line.transform(rdd=>{rdd})
//    .window(Seconds(6), Seconds(2)).foreachRDD(foreachFunc)
          line.foreachRDD(rdd=>{
        println("--------------------------------"+new Date().toLocaleString())
        rdd.foreach(f=>println(f._1+"\t"+f._2+"\t"+new Date().toLocaleString()))
        skm.updateZk(rdd)
      })
   })

}