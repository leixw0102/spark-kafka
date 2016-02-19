package com.ehl.spark.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source
import scala.reflect.io.File
import com.google.common.io.Files

/**
 * @author ehl

 * 
 * driver + executors
 * 
 * driver->SparkContext;
 * 
 * ./bin/spark-submit 
  --class <main-class>
  --master <master-url> 
  --deploy-mode <deploy-mode> 
  ... # other options
  <application-jar> 
  [application-arguments]
   spark-submit --master spark://ehl-test-01:7077  --class com.ehl.spark.example.Example1 --name test-ehl spark-example-0.0.1-SNAPSHOT-jar-with-dependencies.jar hdfs://ehl/app/test
   
   bin/spark-shell --master yarn-client --executor-memory 3g --num-executors 3
 */


object Example3 extends SparkOp{
  def main(args: Array[String]): Unit = {
//    require(args.size==0)
    operateSpark(args){
      sc=>
        
        args.foreach { println }
        
//        val rdd=sc.parallelize(1 to 9)//sc.parallelize(1 to 9,3)
       
        /**
         * 使用textFile()方法可以将本地文件或HDFS文件转换成RDD
支持整个文件目录读取，文件可以是文本或者压缩文件(如gzip等，自动执行解压缩并加载数据)。如textFile（”file:///dfs/data”）
支持通配符读取,例如：
         */
        //126,2015-06-23 00:00:13,无牌,99,4
//        sc.
       //总数"E:\\ehl\\文档\\工作\\研判\\passcar_utf8\\"
//        val r=sc.textFile(args(0)).filter ( x=> !x.isEmpty() && x.split(",").length==5).filter(!_.contains("无牌")).map { x => x.split(",") }
        //date-卡口, 1
//        val whole = r1.map { x => (x.trim().split(" ")(0)) }.map { (_,1)}.reduceByKey(_+_)
        
        
//        date- class
//        println(r.first())
//        var rdd1 =r.map { x=>  (x(1).split(" ")(0),Obj(x(0).toInt,x(1).split(" ")(0),x(2)))}
//        var totalRdd = r.map(x=>x(1).split(" ")(0)).map { (_,1)}.reduceByKey(_+_)       //day --total
//        totalRdd.repartition(1).saveAsTextFile(args(1))
          sc.textFile("").filter { x => ??? }
        
        /**
         * Transformations
         * 1.map 
         * 2.filter
         * 3.flatMap  -- a.map,b，合并
         * 4.map*  -- 
         * 5.sample--采样
         * 6.union  --交集
         * 7.intersection
         * 8.distinct --返回一个包含源数据集中所有不重复元素的新数据集
         * 9.groupByKey-shuffle操作；宽依赖，
         * 10.sortBy
         * 11 join
         */
//        println(rdd.map (_*1).countByValue()) 
        
        /**
         * action
         * 1.collect
         * 2.reduce
         * 3.count
         * 4.first
         * 5.take
         * 6.countByKey
         * 7.foreach
         * saveAsTextFile saveAsSequenceFile
         */
        
        //CACHE DISK  一般的executor内存60%做 cache， 剩下的40%做task
        /**
         * object StorageLevel {
  val NONE = new StorageLevel(false, false, false, false)
  val DISK_ONLY = new StorageLevel(true, false, false, false)
  val DISK_ONLY_2 = new StorageLevel(true, false, false, false, 2)
  val MEMORY_ONLY = new StorageLevel(false, true, false, true)
  val MEMORY_ONLY_2 = new StorageLevel(false, true, false, true, 2)
  val MEMORY_ONLY_SER = new StorageLevel(false, true, false, false)
  val MEMORY_ONLY_SER_2 = new StorageLevel(false, true, false, false, 2)
  val MEMORY_AND_DISK = new StorageLevel(true, true, false, true)
  val MEMORY_AND_DISK_2 = new StorageLevel(true, true, false, true, 2)
  val MEMORY_AND_DISK_SER = new StorageLevel(true, true, false, false)
  val MEMORY_AND_DISK_SER_2 = new StorageLevel(true, true, false, false, 2)
  val OFF_HEAP = new StorageLevel(false, false, true, false) // Tachyon
}
class StorageLevel private(  
     private var useDisk_      :    Boolean,  
     private var useMemory_   :  Boolean,  
     private var useOffHeap_   : Boolean,  
     private var deserialized_ : Boolean,  
     private var replication_  : Int = 1
)
         */
//        rdd.cache()
    }
    
  }
  
 
}