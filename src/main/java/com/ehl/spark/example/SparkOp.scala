package com.ehl.spark.example

import org.apache.spark._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

/**
 * @author ehl
 */
trait SparkOp {
  
  
  /**
   * sparkContext 
   *  
   */
  def operateSpark(args:Array[String])(op:(SparkContext)=>Unit){
    
   //first
    val conf=new SparkConf().setAppName("example1").setMaster("local[*]")
    //second :init spark context
    val sc = new SparkContext(conf)
    try{
      op(sc)
    }catch{
      case ex:Exception=>ex.printStackTrace()
    } finally{
      //end
      sc.stop()
    }
  }
}