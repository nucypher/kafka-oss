package kafka.nucypher

import java.nio.ByteBuffer
import java.util

import com.nucypher.kafka.clients.MessageHandler
import com.nucypher.kafka.clients.granular.{StructuredDataAccessor, StructuredMessageHandler}
import com.nucypher.kafka.encrypt.DataEncryptionKeyManager
import com.nucypher.kafka.errors.CommonException
import com.nucypher.kafka.utils.WrapperReEncryptionKey
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
    * Creates new instance of ReEncryptionHandler. If 'nucypher.reencryption.keys.path' is empty then returns stub
    *
    * @param zkUtils ZooKeeper connection
    * @param config  Kafka configuration
    * @return instance of ReEncryptionHandler
    */
  def apply(zkUtils: ZkUtils, config: KafkaConfig): ReEncryptionHandler =
    if (config.nuCypherReEncryptionKeysPath == null)
      new ReEncryptionHandlerStub
    else
      new ReEncryptionHandlerImpl(
        zkUtils,
        config.nuCypherReEncryptionKeysPath,
        config.nuCypherCacheReEncryptionKeysCapacity,
        config.nuCypherCacheGranularReEncryptionKeysCapacity,
        config.nuCypherCacheEDEKCapacity,
        config.nuCypherCacheChannelsCapacity,
        config.nuCypherCacheChannelsTTLms,
        config.originals()
      )

  /**
    * Make simple LRU and TTL cache
    *
    * @param capacity    cache size
    * @param ttl         time to live in milliseconds
    * @param getFunction function for getting element
    * @tparam K key type
    * @tparam V value type
    * @return cacheable map with timestamps
    */
  def makeCache[K, V](capacity: Int, ttl: Long, getFunction: K => V): mutable.Map[K, (V, Long)] = {
    makeCache[K, (V, Long)](
      capacity,
      (v: (V, Long)) => System.currentTimeMillis() - v._2 > ttl,
      (k: K) => {
        val value = getFunction(k)
        if (value != null) (value, System.currentTimeMillis()) else null
      }
    )
  }

  /**
    * Make simple LRU cache
    *
    * @param capacity    cache size
    * @param getFunction function for getting element
    * @tparam K key type
    * @tparam V value type
    * @return cacheable map
    */
  def makeCache[K, V](capacity: Int, getFunction: K => V): mutable.Map[K, V] = {
    makeCache(capacity, _ => false, getFunction)
  }

  /**
    * Make simple LRU cache with filtering
    *
    * @param capacity       cache size
    * @param filterFunction function for filtering values
    * @param getFunction    function for getting element
    * @tparam K key type
    * @tparam V value type
    * @return cacheable map
    */
  def makeCache[K, V](capacity: Int,
                      filterFunction: V => Boolean,
                      getFunction: K => V): mutable.Map[K, V] = {
    import scala.collection.JavaConverters._
    new util.LinkedHashMap[K, V](capacity, 0.7F, true) {
      private val cacheCapacity = capacity

      override def removeEldestEntry(entry: util.Map.Entry[K, V]): Boolean = {
        this.size() > this.cacheCapacity || filterFunction(entry.getValue)
      }

      override def get(key: scala.Any): V = this.synchronized {
        var value = super.get(key)
        if (value == null) {
          value = getFunction(key.asInstanceOf[K])
        } else if (filterFunction(value)) {
          this.remove(key.asInstanceOf[K])
          value = null.asInstanceOf[V]
        }
        if (value != null && filterFunction(value)) {
          value = null.asInstanceOf[V]
        }
        if (value != null) {
          this.put(key.asInstanceOf[K], value)
        }
        value
      }

    }.asScala
  }

  //TODO change to cache library
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
  * @param granularReEncryptionKeysCacheCapacity granular re-encryption keys cache capacity
  * @param edekCacheCapacity                     EDEK cache capacity
  * @param channelsCacheCapacity                 channels cache capacity
  * @param channelsCacheTTLms                    channels cache TTL
  */
class ReEncryptionHandlerImpl(zkUtils: ZkUtils,
                              keysRootPath: String,
                              reEncryptionKeysCacheCapacity: Int,
                              granularReEncryptionKeysCacheCapacity: Int,
                              edekCacheCapacity: Int,
                              channelsCacheCapacity: Int,
                              channelsCacheTTLms: Long,
                              props: java.util.Map[String, _]
                             ) extends ReEncryptionHandler with Logging {

  private val zkHandler = new BaseZooKeeperHandler(zkUtils.zkConnection.getZookeeper, keysRootPath)
  private val keyManager = new DataEncryptionKeyManager(edekCacheCapacity)
  private val messageHandler = new MessageHandler(keyManager)
  private val reEncryptionKeysCache =
    ReEncryptionHandler.makeCache[(String, String, ClientType), KeyHolder](
      reEncryptionKeysCacheCapacity,
      (v: KeyHolder) => v.isExpired,
      (k: (String, String, ClientType)) => zkHandler.getKey(k._1, k._2, k._3)
    )
  private val granularReEncryptionKeysCache =
    ReEncryptionHandler.makeCache[(String, String, ClientType, String), KeyHolder](
      granularReEncryptionKeysCacheCapacity,
      (v: KeyHolder) => v.isExpired,
      (k: (String, String, ClientType, String)) => zkHandler.getKey(k._1, k._2, k._3, k._4)
    )
  private val channelsCache = ReEncryptionHandler
    .makeCache[String, (Option[Channel], Option[StructuredDataAccessor])](
    channelsCacheCapacity,
    channelsCacheTTLms,
    (k: String) => {
      val channel = zkHandler.getChannel(k)
      if (channel == null) (None, None)
      else {
        val accessor =
          if (channel.getType == EncryptionType.GRANULAR) {
            val dataAccessor = channel.getAccessorClass.newInstance()
            dataAccessor.configure(props, false)
            Some(dataAccessor)
          } else {
            None
          }
        (Some(channel), accessor)
      }
    }
  )

  this.logIdent = "[ReEncryptionHandler-%s] ".format(keysRootPath)

  /**
    * Checks topic encryption
    *
    * @param topic topic to check
    * @return result of checking
    */
  def isTopicEncrypted(topic: String): Boolean = channelsCache.get(topic).isDefined &&
    channelsCache(topic)._1._1.isDefined

  /**
    * Checks rights for re-encryption records
    *
    * @param topic      topic name to check
    * @param principal  user to check
    * @param clientType client type to check
    * @return result of checking
    */
  def isAllowReEncryption(topic: String, principal: KafkaPrincipal, clientType: ClientType): Boolean = {
    channelsCache(topic)._1._1.isDefined &&
      (channelsCache(topic)._1._1.get.getType == EncryptionType.GRANULAR ||
        (reEncryptionKeysCache.get((topic, principal.getName, clientType)) match {
          case Some(key) => !key.isExpired
          case None => false
        }))
  }

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
      if (!records.records().iterator().hasNext || !isTopicEncrypted(topicPartition.topic())) {
        debug(s"Records to the topic '${topicPartition.topic()}' without encryption")
        reEncrypted += topicPartition -> records
      } else if (!isAllowReEncryption(topicPartition.topic(), principal, ClientType.PRODUCER)) {
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
      if (!data.records.records().iterator().hasNext || !isTopicEncrypted(topicPartition.topic)) {
        debug(s"Records from the topic '${topicPartition.topic}' without encryption")
        topicPartition -> data
      } else if (!isAllowReEncryption(topicPartition.topic, principal, ClientType.CONSUMER)) {
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
    * @param topic         topic name
    * @param principalName user principal name
    * @param clientType    client type
    * @param records       records
    * @return re-encrypted records
    */
  def reEncryptTopic(topic: String,
                     principalName: String,
                     clientType: ClientType,
                     records: Records): Records = {
    val channel = channelsCache(topic)._1
    channel._1.get.getType match {
      case EncryptionType.GRANULAR =>
        granularReEncryptTopic(
          channel._1.get, channel._2.get, principalName, clientType, records)
      case EncryptionType.FULL =>
        fullReEncryptTopic(channel._1.get, principalName, clientType, records)
    }
  }

  /**
    * Granular re-encrypt all messages in one topic by fields
    *
    * @param channel       channel object
    * @param accessor      data accessor
    * @param principalName user principal name
    * @param clientType    client type
    * @param records       records
    * @return re-encrypted records
    */
  def granularReEncryptTopic(channel: Channel,
                             accessor: StructuredDataAccessor,
                             principalName: String,
                             clientType: ClientType,
                             records: Records): MemoryRecords = {
    reEncryptTopic(
      bytes => reEncryptPayload(bytes, channel, accessor, principalName, clientType),
      records
    )
  }

  /**
    * Full re-encrypt all records in one topic
    *
    * @param channel       channel object
    * @param principalName user principal name
    * @param clientType    client type
    * @param records       records
    * @return re-encrypted records
    */
  def fullReEncryptTopic(channel: Channel,
                         principalName: String,
                         clientType: ClientType,
                         records: Records): Records = {
    val topic = channel.getName
    val keyHolder = reEncryptionKeysCache((topic, principalName, clientType))
    if (keyHolder.getKey.isEmpty) {
      debug(s"Records in the topic '$topic' for ${clientType.toString.toLowerCase} " +
        s"'$principalName' are not re-encrypted")
      records
    } else {
      debug(s"Re-encrypt records in the topic '$topic' " +
        s"for ${clientType.toString.toLowerCase} '$principalName'")
      reEncryptTopic(
        bytes => reEncryptPayload(topic, bytes, keyHolder.getKey),
        records)
    }
  }

  /**
    * Re-encrypt all records in one topic
    *
    * @param reEncryptFunction re-encryption function
    * @param records           records
    * @return re-encrypted records
    */
  def reEncryptTopic(reEncryptFunction: Array[Byte] => Array[Byte],
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
        builder.append(reEncryptRecord(reEncryptFunction, record))
      }
    }

    builder.closeForRecordAppends()
    builder.build()
  }

  /**
    * Re-encrypt one record
    *
    * @param reEncryptFunction re-encryption function
    * @param record            record
    * @return re-encrypted payload
    */
  def reEncryptRecord(reEncryptFunction: Array[Byte] => Array[Byte],
                      record: Record): SimpleRecord = {
    val payload = record.value
    var valueBytes: Array[Byte] = null
    if (payload != null) {
      valueBytes = new Array[Byte](payload.remaining)
      payload.get(valueBytes)
      valueBytes = reEncryptFunction(valueBytes)
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

  /**
    * Full re-encrypt payload from record
    *
    * @param topic   topic
    * @param payload record payload
    * @param reKey   re-encryption key
    * @return re-encrypted payload
    */
  def reEncryptPayload(topic: String, payload: Array[Byte], reKey: WrapperReEncryptionKey): Array[Byte] = {
    messageHandler.reEncrypt(topic, payload, reKey)
  }

  /**
    * Granular re-encrypt payload from record
    *
    * @param payload structured payload
    * @param channel channel object
    * @return re-encrypted payload
    */
  def reEncryptPayload(payload: Array[Byte],
                       channel: Channel,
                       accessor: StructuredDataAccessor,
                       principalName: String,
                       clientType: ClientType): Array[Byte] = {
    import scala.collection.JavaConverters._

    val topic = channel.getName
    val structuredMessageHandler = new StructuredMessageHandler(messageHandler)
    val reKeys = structuredMessageHandler.getAllEncryptedFields(payload).asScala.flatMap {
      case (field) =>
        val reKey = granularReEncryptionKeysCache.get(
          (topic, principalName, clientType, field))
        if (reKey.isEmpty && clientType == ClientType.PRODUCER) {
          throw new CommonException(s"There are no re-encryption key " +
            s"for ${clientType.toString.toLowerCase} '$principalName', " +
            s"channel '$topic' and field '$field'")
        }
        if (reKey.isEmpty || reKey.get.getKey.isEmpty) {
          //TODO change to trace
          debug(s"Field '$field' in the message in the topic '$topic' " +
            s"for ${clientType.toString.toLowerCase} '$principalName' is not re-encrypted")
          None
        } else {
          Some(field, reKey.get.getKey)
        }
    }.toMap.asJava
    structuredMessageHandler.reEncrypt(topic, reKeys)
  }

}