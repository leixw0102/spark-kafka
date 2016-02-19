package com.ehl.spark.example

import org.apache.spark.streaming.StreamingContext
import org.apache.spark._
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.Seconds

/**
 * @author ehl
 */
trait SparkStreamOp {
  /**
   * stream context;
   * args
   * 0--second
   */
  def operateStreamSpark(args:Array[String])(op:(StreamingContext) =>Unit){
     //first
    val conf=new SparkConf().setAppName("example1")
    //second :init spark context
    val sc = new SparkContext(conf)
    
    val streamC = new StreamingContext(sc,Seconds(10))
    try{
      streamC.checkpoint("checkpoint")
      op(streamC)
      streamC.start()
      streamC.awaitTermination()
    }catch{
      case ex:Exception=>ex.printStackTrace()
    } finally{
      //end
      //sc.stop()
    }
  }
  
}