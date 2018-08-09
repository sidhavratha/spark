/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.kafka010

import java.{util => ju}

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.{KafkaException, TopicPartition}

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.kafka010.consumer.SparkKafkaConsumer


/**
 * Consumer of single topicpartition, intended for cached reuse.
 * Underlying consumer is not threadsafe, so neither is this,
 * but processing the same topicpartition and group id in multiple threads is usually bad anyway.
 */
private[kafka010]
class CachedKafkaConsumer[K, V] private(
  val groupId: String,
  val topic: String,
  val partition: Int,
  val kafkaParams: ju.Map[String, Object],
  val sparkKafkaConsumer: SparkKafkaConsumer[K, V]) extends Logging {

  assert(groupId == kafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG),
    "groupId used for cache key must match the groupId in kafkaParams")

  val topicPartition = new TopicPartition(topic, partition)

  def close(): Unit = sparkKafkaConsumer.close()

  /**
   * Get the record for the given offset, waiting up to timeout ms if IO is necessary.
   * Sequential forward access will use buffers, but random access will be horribly inefficient.
   */
  def get(offset: Long): ConsumerRecord[K, V] = {
    logTrace(s"Get $groupId $topicPartition nextOffset "
      + sparkKafkaConsumer.getNextOffset()
      + " requested $offset")
    if (offset != sparkKafkaConsumer.getNextOffset()) {
      logInfo(s"Initial fetch for $groupId $topicPartition $offset")
      sparkKafkaConsumer.ensureOffset(offset)
    }

    var record = sparkKafkaConsumer.getNextRecord()
    require(record != null,
      s"Failed to get records for $groupId $topicPartition $offset after polling for "
        + sparkKafkaConsumer.getTimeout())

    if (record.offset != offset) {
      sparkKafkaConsumer.ensureOffset(offset)
      record = sparkKafkaConsumer.getNextRecord()
      require(record != null,
        s"Failed to get records for $groupId $topicPartition $offset after polling for "
          + sparkKafkaConsumer.getTimeout())
    }
    record
  }
}

private[kafka010]
object CachedKafkaConsumer extends Logging {

  private case class CacheKey(groupId: String, topic: String, partition: Int)

  // Don't want to depend on guava, don't want a cleanup thread, use a simple LinkedHashMap
  private var cache: ju.LinkedHashMap[CacheKey, CachedKafkaConsumer[_, _]] = null

  /** Must be called before get, once per JVM, to configure the cache. Further calls are ignored */
  def init(
      initialCapacity: Int,
      maxCapacity: Int,
      loadFactor: Float): Unit = CachedKafkaConsumer.synchronized {
    if (null == cache) {
      logInfo(s"Initializing cache $initialCapacity $maxCapacity $loadFactor")
      cache = new ju.LinkedHashMap[CacheKey, CachedKafkaConsumer[_, _]](
        initialCapacity, loadFactor, true) {
        override def removeEldestEntry(
          entry: ju.Map.Entry[CacheKey, CachedKafkaConsumer[_, _]]): Boolean = {
          if (this.size > maxCapacity) {
            try {
              entry.getValue.sparkKafkaConsumer.close()
            } catch {
              case x: KafkaException =>
                logError("Error closing oldest Kafka consumer", x)
            }
            true
          } else {
            false
          }
        }
      }
    }
  }

  /**
   * Get a cached consumer for groupId, assigned to topic and partition.
   * If matching consumer doesn't already exist, will be created using kafkaParams.
   */
  def get[K, V](
      groupId: String,
      topic: String,
      partition: Int,
      kafkaParams: ju.Map[String, Object],
      sparkKafkaConsumer: SparkKafkaConsumer[K, V]): CachedKafkaConsumer[K, V] =
    CachedKafkaConsumer.synchronized {
      val k = CacheKey(groupId, topic, partition)
      val v = cache.get(k)
      if (null == v) {
        logInfo(s"Cache miss for $k")
        logDebug(cache.keySet.toString)
        val c = new CachedKafkaConsumer[K, V](
          groupId, topic, partition, kafkaParams, sparkKafkaConsumer
        )
        cache.put(k, c)
        c
      } else {
        // any given topicpartition should have a consistent key and value type
        v.asInstanceOf[CachedKafkaConsumer[K, V]]
      }
    }

  /**
   * Get a fresh new instance, unassociated with the global cache.
   * Caller is responsible for closing
   */
  def getUncached[K, V](
      groupId: String,
      topic: String,
      partition: Int,
      kafkaParams: ju.Map[String, Object],
      sparkKafkaConsumer: SparkKafkaConsumer[K, V]): CachedKafkaConsumer[K, V] =
    new CachedKafkaConsumer[K, V](groupId, topic, partition, kafkaParams, sparkKafkaConsumer)

  /** remove consumer for given groupId, topic, and partition, if it exists */
  def remove(groupId: String, topic: String, partition: Int): Unit = {
    val k = CacheKey(groupId, topic, partition)
    logInfo(s"Removing $k from cache")
    val v = CachedKafkaConsumer.synchronized {
      cache.remove(k)
    }
    if (null != v) {
      v.close()
      logInfo(s"Removed $k from cache")
    }
  }
}
