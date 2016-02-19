package org.apache.spark.streaming.kafka

import scala.reflect.ClassTag
import kafka.serializer.Decoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import kafka.message.MessageAndMetadata
import kafka.common.TopicAndPartition
import org.apache.spark.rdd.RDD
import com.ehl.spark.example.KafkaOffsetsPersistent
import org.apache.spark.SparkException
import scala.util.Try
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import kafka.common.TopicAndPartition
import scala.collection.mutable.ArrayBuffer
import scala.util.Success
import scala.util.Failure

/**
 * @author ehl
 * 记录spark消费kafka，写入zk
 */

class SparkKafka2ZkManager(var kafkaParams: Map[String, String]) extends Serializable with KafkaOffsetsPersistent {
	
    private type Err = ArrayBuffer[Throwable]
		private	val groupId=kafkaParams.getOrElse("group.id", "spark-kafka-10000000");
	  kafkaParams+=("spark.streaming.kafka.maxRatePerPartition"->"500"); //500条
	  private val batchSize= kafkaParams.get("spark.streaming.kafka.maxRetries")
			private val timeInterval=kafkaParams.getOrElse("spark.stream.kafka.time.interval", 2000L)//2000毫秒
			private var _lastUpdateMs=0L
      private val kc = new KafkaCluster(kafkaParams)
			/**
			 * 对kafkautils.createDirectStream部分使用重写
			 */
			def createKafkaDirectStream[
        K: ClassTag,
        V: ClassTag, 
        KD <: Decoder[K]: ClassTag, 
        VD <: Decoder[V]: ClassTag](
        		ssc: StreamingContext,
        		topics: Set[String]
        		): InputDStream[(K, V)] ={

        				//获取offset 进行判断是否获取历史消息；并返回位置
        				val fromOffsets = zkSetOrUpdate(topics)
                
                println(fromOffsets.size+"+++++++++++++++++++++++++++"+fromOffsets.toString())

        				val messageHandler = (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message)
        				KafkaUtils.createDirectStream[K, V, KD, VD, (K, V)](ssc, kafkaParams, fromOffsets, messageHandler);
	} 
	/**
	 * 设置或者更新offsets
	 */
	def zkSetOrUpdate(topics: Set[String]) :Map[TopicAndPartition,Long]= {
       var map =Map[TopicAndPartition,Long]();
			topics.foreach { topic => {
				
        val partions=kc.getPartitions(Set(topic));
        
        if(partions.isLeft) throw new SparkException(s"get kafka partition failed:${partions.left.get}")
        val consumerOffsets=kc.getConsumerOffsets(groupId, partions.right.get);
        
			  consumerOffsets match{
          case Left(s)=>{
            val reset = kafkaParams.getOrElse("auto.offset.reset","largest").toLowerCase()
            var leaderOffsets: Map[TopicAndPartition, LeaderOffset] = null
            if (reset == Some("smallest")) {
              leaderOffsets = kc.getEarliestLeaderOffsets(partions.right.get).right.get
            } else {
              println(".............largest...")
              leaderOffsets = kc.getLatestLeaderOffsets(partions.right.get).right.get
              println(leaderOffsets.toString())
            }
            val offsets = leaderOffsets.map {
              
                case (tp, offset) => map+=(tp-> offset.offset);(tp, offset.offset)
              }
              kc.setConsumerOffsets(groupId, offsets)
              }
              
          case Right(r)=>{
            println("right....")
              /**
               * 如果zk上保存的offsets已经过时了，即kafka的定时清理策略已经将包含该offsets的文件删除。
               * 针对这种情况，只要判断一下zk上的consumerOffsets和earliestLeaderOffsets的大小，
               * 如果consumerOffsets比earliestLeaderOffsets还小的话，说明consumerOffsets已过时,
               * 这时把consumerOffsets更新为earliestLeaderOffsets
               */
              val earliestLeaderOffsets = kc.getEarliestLeaderOffsets(partions.right.get).right.get
               var offsets: Map[TopicAndPartition, Long] = Map()
                  r.foreach({ case(tp, n) =>
                  val earliestLeaderOffset =earliestLeaderOffsets(tp).offset
                  if (n < earliestLeaderOffset) {
                    println("consumer group:" + groupId + ",topic:" + tp.topic + ",partition:" + tp.partition +
                        " offsets已经过时，更新为" + earliestLeaderOffset)
                    map += (tp -> earliestLeaderOffset)
                    kc.setConsumerOffsets(groupId, Map(tp -> earliestLeaderOffset))
                  }else{
                    map+=(tp->n)
                  }
                  })
//                  if (!map.isEmpty) {
//                    kc.setConsumerOffsets(groupId, map)
//                  }
          }
        }
				
			} 
      }
        map
	}
//	/**
//	 * 获取kafka 消费位置
//	 */
//	def getTopicAndPation(topics: Set[String]) :Try[(Set[TopicAndPartition],Map[TopicAndPartition,Long])]= {
//			val partions=kc.getPartitions(topics);
//			if(partions.isLeft) throw new SparkException(s"get kafka partition failed:${partions.left.get}")
//			 val consumerOffsets=kc.getConsumerOffsets(groupId, partions.right.get);
//			
//			 Try(partions.right.get,consumerOffsets.right.get)
//	}
	/**
	 * 更新
	 */
	def updateZk[K:ClassTag, V:ClassTag](rdd: RDD[(K, V)]): Unit = {

			val current=System.nanoTime()

					if(current-_lastUpdateMs >= timeInterval.toString().toLong){

						val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

								for (offsets <- offsetsList) {
									val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
											val o = kc.setConsumerOffsets(groupId, Map((topicAndPartition, offsets.untilOffset)))
											if (o.isLeft) {
												println(s"Error updating the offset to Kafka cluster: ${o.left.get}")
											}
								}
            _lastUpdateMs=current;
					}
	}
}

object SparkKafka2ZkManager{
	def apply(kafkaParams: Map[String, String]):SparkKafka2ZkManager={
			new SparkKafka2ZkManager(kafkaParams);
	}
}