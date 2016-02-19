package com.ehl.spark.example

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD

/**
 * @author ehl
 */
trait KafkaOffsetsPersistent {
  def updateZk[K:ClassTag, V:ClassTag](rdd: RDD[(K, V)])
}