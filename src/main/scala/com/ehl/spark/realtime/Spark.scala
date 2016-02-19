package com.ehl.spark.realtime

import com.ehl.spark.example.SparkStreamOp
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.Minutes
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.OffsetRange
import org.apache.spark.streaming.kafka.HasOffsetRanges

/**
 * @author ehl
 */
object Spark extends SparkStreamOp with App{
   operateStreamSpark(args)(sc=>{

  val topicsSet = Set("page_visits");//.toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "host215,host234")
     val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      sc, kafkaParams, topicsSet)
      
    //val messages = KafkaUtils.createDirectStream(sc,kafkaParams,topicsSet);//.map(_._2)//(sc, "host219:2181", "test-1", Map("page_visits"->4)).map(_._2)
//    
    val line =messages.map(x=>x._2);
//    val words = messages.map().flatMap(_.split(" "))
//    EhlSparkZk().updateZk(rdd)
//    val wordCounts = words.map(x => (x, 1L))
//      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(1), Seconds(10))
    line.print()
//    wordCounts.

   })
}


