package kafka.nucypher

import java.nio.ByteBuffer

import com.nucypher.kafka.zk._
import kafka.message._
import kafka.server.KafkaConfig
import kafka.utils.{Logging, ZkUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.FetchResponse
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.security.auth.KafkaPrincipal

import scala.collection.{Seq, mutable}
import scala.util.{Failure, Success, Try}

/**
  * Base class for re-encryption
  */
trait ReEncryptionHandler {

  /**
    * Re-encrypt all records from producer
    *
    * @param requestInfo request info
    * @param principal   principal
    * @return re-encrypted data and errors
    */
  def reEncryptProducer(requestInfo: mutable.Map[TopicPartition, MemoryRecords],
                        principal: KafkaPrincipal):
  (mutable.Map[TopicPartition, MemoryRecords], mutable.Map[TopicPartition, PartitionResponse])

  /**
    * Re-encrypt all records for consumer
    *
    * @param partitionData fetched data
    * @param principal     principal
    * @return re-encrypted data
    */
  def reEncryptConsumer(partitionData: Seq[(TopicPartition, FetchResponse.PartitionData)],
                        principal: KafkaPrincipal): Seq[(TopicPartition, FetchResponse.PartitionData)]
}

/**
  * ReEncryptionHandler companion
  */
object ReEncryptionHandler {

  /**
    * Creates new instance of ReEncryptionHandler. If 'reencryption.keys.path' is empty then returns stub
    *
    * @param zkUtils ZooKeeper connection
    * @param config  Kafka configuration
    * @return instance of ReEncryptionHandler
    */
  def apply(zkUtils: ZkUtils, config: KafkaConfig): ReEncryptionHandler =
    if (config.reEncryptionKeysPath == null)
      new ReEncryptionHandlerStub
    else
      new ReEncryptionHandlerImpl(
        zkUtils,
        config.reEncryptionKeysPath,
        config.reEncryptionCacheKeysCapacity,
        config.reEncryptionCacheKeysTTLms,
        config.reEncryptionCacheGranularKeysCapacity,
        config.reEncryptionCacheGranularKeysTTLms,
        config.reEncryptionCacheEDEKCapacity,
        config.reEncryptionCacheChannelsCapacity,
        config.reEncryptionCacheChannelsTTLms,
        config.originals()
      )

}

/**
  * Stub class used for disabling re-encryption
  */
class ReEncryptionHandlerStub() extends ReEncryptionHandler {

  /**
    * Convert [[ByteBuffer]] to [[ByteBufferMessageSet]]
    *
    * @param requestInfo request info
    * @param principal   principal (not used)
    * @return records per partition
    */
  override def reEncryptProducer(requestInfo: mutable.Map[TopicPartition, MemoryRecords],
                                 principal: KafkaPrincipal):
  (mutable.Map[TopicPartition, MemoryRecords], mutable.Map[TopicPartition, PartitionResponse]) = {
    (requestInfo, mutable.Map[TopicPartition, PartitionResponse]())
  }

  /**
    * Return input data
    *
    * @param partitionData fetched data
    * @param principal     principal (not used)
    * @return fetched data
    */
  override def reEncryptConsumer(partitionData: Seq[(TopicPartition, FetchResponse.PartitionData)],
                                 principal: KafkaPrincipal):
  Seq[(TopicPartition, FetchResponse.PartitionData)] = partitionData

}

/**
  * Helper class
  *
  * @param zkUtils                               instance of [[ZkUtils]]
  * @param keysRootPath                          keys root path in ZooKeeper
  * @param reEncryptionKeysCacheCapacity         re-encryption keys cache capacity
  * @param reEncryptionKeysCacheTTLms            re-encryption keys cache TTL
  * @param granularReEncryptionKeysCacheCapacity granular re-encryption keys cache capacity
  * @param granularReEncryptionKeysCacheTTLms    granular re-encryption keys cache TTL
  * @param edekCacheCapacity                     EDEK cache capacity
  * @param channelsCacheCapacity                 channels cache capacity
  * @param channelsCacheTTLms                    channels cache TTL
  */
class ReEncryptionHandlerImpl(zkUtils: ZkUtils,
                              keysRootPath: String,
                              reEncryptionKeysCacheCapacity: Int,
                              reEncryptionKeysCacheTTLms: Long,
                              granularReEncryptionKeysCacheCapacity: Int,
                              granularReEncryptionKeysCacheTTLms: Long,
                              edekCacheCapacity: Int,
                              channelsCacheCapacity: Int,
                              channelsCacheTTLms: Long,
                              props: java.util.Map[String, _]
                             ) extends ReEncryptionHandler with Logging {

  private val handler = new com.nucypher.kafka.clients.ReEncryptionHandler(
    zkUtils.zkConnection.getZookeeper,
    keysRootPath,
    edekCacheCapacity,
    reEncryptionKeysCacheCapacity,
    reEncryptionKeysCacheTTLms,
    granularReEncryptionKeysCacheCapacity,
    granularReEncryptionKeysCacheTTLms,
    channelsCacheCapacity,
    channelsCacheTTLms
  )

  this.logIdent = "[ReEncryptionHandler-%s] ".format(keysRootPath)


  /**
    * Re-encrypt all records from producer
    *
    * @param requestInfo request info
    * @param principal   principal
    * @return re-encrypted data and errors
    */
  def reEncryptProducer(requestInfo: mutable.Map[TopicPartition, MemoryRecords],
                        principal: KafkaPrincipal):
  (mutable.Map[TopicPartition, MemoryRecords], mutable.Map[TopicPartition, PartitionResponse]) = {
    debug(s"Re-encrypt all records for producer '${principal.getName}'")
    val reEncrypted = new mutable.HashMap[TopicPartition, MemoryRecords]
    val errors = new mutable.HashMap[TopicPartition, PartitionResponse]

    for {(topicPartition, records) <- requestInfo} {
      if (!records.records().iterator().hasNext ||
        !handler.isTopicEncrypted(topicPartition.topic(), principal.getName, ClientType.PRODUCER)) {
        debug(s"Records to the topic '${topicPartition.topic()}' without encryption")
        reEncrypted += topicPartition -> records
      } else if (!handler.isAllowReEncryption(topicPartition.topic(), principal.getName, ClientType.PRODUCER)) {
        error(s"Producer '${principal.getName}' not authorized for writing " +
          s"records to the topic '${topicPartition.topic()}'")
        errors += topicPartition -> new PartitionResponse(Errors.UNKNOWN)
      } else Try(reEncryptTopic(topicPartition.topic, principal.getName, ClientType.PRODUCER, records)) match {
        case Success(x) => reEncrypted += topicPartition -> x.asInstanceOf[MemoryRecords] //TODO fix asInstanceOf
        case Failure(e) => error(s"Error while re-encryption topic-partition " +
          s"${topicPartition.topic()}-${topicPartition.partition()}", e)
          errors += topicPartition -> new PartitionResponse(Errors.UNKNOWN)
      }
    }

    (reEncrypted, errors)
  }

  /**
    * Re-encrypt all records for consumer
    *
    * @param partitionData fetched data
    * @param principal     principal
    * @return re-encrypted data
    */
  def reEncryptConsumer(partitionData: Seq[(TopicPartition, FetchResponse.PartitionData)],
                        principal: KafkaPrincipal): Seq[(TopicPartition, FetchResponse.PartitionData)] = {
    debug(s"Re-encrypt all records for consumer '${principal.getName}'")
    partitionData.map { case (topicPartition, data) =>
      if (!data.records.records().iterator().hasNext ||
        !handler.isTopicEncrypted(topicPartition.topic, principal.getName, ClientType.CONSUMER)) {
        debug(s"Records from the topic '${topicPartition.topic}' without encryption")
        topicPartition -> data
      } else if (!handler.isAllowReEncryption(topicPartition.topic, principal.getName, ClientType.CONSUMER)) {
        error(s"Consumer '${principal.getName}' not authorized for fetching " +
          s"records from the topic '${topicPartition.topic}'")
        topicPartition -> new FetchResponse.PartitionData(
          Errors.UNKNOWN,
          FetchResponse.INVALID_HIGHWATERMARK,
          FetchResponse.INVALID_LAST_STABLE_OFFSET,
          FetchResponse.INVALID_LOG_START_OFFSET,
          null,
          MemoryRecords.EMPTY)
      } else Try(reEncryptTopic(topicPartition.topic, principal.getName, ClientType.CONSUMER, data.records)) match {
        case Success(x) => topicPartition -> new FetchResponse.PartitionData(
          data.error,
          data.highWatermark,
          data.lastStableOffset,
          data.logStartOffset,
          data.abortedTransactions,
          x)
        case Failure(e) => error(s"Error while re-encryption topic-partition " +
          s"${topicPartition.topic}-${topicPartition.partition}", e)
          topicPartition -> new FetchResponse.PartitionData(
            Errors.UNKNOWN,
            FetchResponse.INVALID_HIGHWATERMARK,
            FetchResponse.INVALID_LAST_STABLE_OFFSET,
            FetchResponse.INVALID_LOG_START_OFFSET,
            null,
            MemoryRecords.EMPTY)
      }
    }
  }

  /**
    * Re-encrypt all records in one topic
    *
    * @param topic         topic
    * @param principalName principal name
    * @param clientType    client type
    * @param records       records
    * @return re-encrypted records
    */
  def reEncryptTopic(topic: String,
                     principalName: String,
                     clientType: ClientType,
                     records: Records): MemoryRecords = {
    import scala.collection.JavaConverters._

    var builder: MemoryRecordsBuilder = null
    for (recordBatch: RecordBatch <- records.batches().asScala) {
      if (builder == null) {
        //assume that all batches has the same compression and others parameters
        builder = MemoryRecords.builder(
          ByteBuffer.allocate(records.sizeInBytes()),
          recordBatch.compressionType(),
          recordBatch.timestampType(),
          recordBatch.baseOffset())
      }
      for (record: Record <- recordBatch.asScala) {
        builder.append(reEncryptRecord(
          topic, principalName, clientType, record))
      }
    }

    builder.closeForRecordAppends()
    builder.build()
  }

  /**
    * Re-encrypt one record
    *
    * @param topic         topic
    * @param principalName principal name
    * @param clientType    client type
    * @param record        record
    * @return re-encrypted payload
    */
  def reEncryptRecord(topic: String,
                      principalName: String,
                      clientType: ClientType,
                      record: Record): SimpleRecord = {
    val payload = record.value
    var valueBytes: Array[Byte] = null
    if (payload != null) {
      valueBytes = new Array[Byte](payload.remaining)
      payload.get(valueBytes)
      valueBytes = handler.reEncryptTopic(
        valueBytes, topic, principalName, clientType)
    }

    val key = record.key
    var keyBytes: Array[Byte] = null
    if (key != null) {
      keyBytes = new Array[Byte](key.remaining)
      key.get(keyBytes)
    }

    new SimpleRecord(
      record.timestamp(),
      keyBytes,
      valueBytes,
      record.headers()
    )
  }

}