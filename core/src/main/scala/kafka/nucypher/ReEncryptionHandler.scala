package kafka.nucypher

import java.nio.ByteBuffer
import java.util

import com.nucypher.kafka.clients.MessageHandler
import com.nucypher.kafka.clients.granular.{StructuredDataAccessor, StructuredMessageHandler}
import com.nucypher.kafka.encrypt.DataEncryptionKeyManager
import com.nucypher.kafka.errors.CommonException
import com.nucypher.kafka.utils.WrapperReEncryptionKey
import com.nucypher.kafka.zk._
import kafka.api.FetchResponsePartitionData
import kafka.common.TopicAndPartition
import kafka.log.FileMessageSet
import kafka.message._
import kafka.server.KafkaConfig
import kafka.utils.{Logging, ZkUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.security.auth.KafkaPrincipal

import scala.collection.mutable.ArrayBuffer
import scala.collection.{Seq, mutable}
import scala.util.{Failure, Success, Try}

/**
  * Base class for re-encryption
  */
trait ReEncryptionHandler {

  /**
    * Re-encrypt all messages from producer
    *
    * @param requestInfo request info
    * @param principal   principal
    * @return re-encrypted data and errors
    */
  def reEncryptProducer(requestInfo: mutable.Map[TopicPartition, ByteBuffer],
                        principal: KafkaPrincipal):
  (mutable.Map[TopicPartition, MessageSet], mutable.Map[TopicPartition, PartitionResponse])

  /**
    * Re-encrypt all messages for consumer
    *
    * @param responsePartitionData fetched data
    * @param principal             principal
    * @return re-encrypted data
    */
  def reEncryptConsumer(responsePartitionData: Seq[(TopicAndPartition, FetchResponsePartitionData)],
                        principal: KafkaPrincipal): Seq[(TopicAndPartition, FetchResponsePartitionData)]
}

/**
  * ReEncryptionHandler companion
  */
object ReEncryptionHandler {

  val DEFAULT_TTL: Long = 4 * 60 * 60 * 1000 //TODO move to configuration

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
    * @return messages per partition
    */
  override def reEncryptProducer(requestInfo: mutable.Map[TopicPartition, ByteBuffer],
                                 principal: KafkaPrincipal):
  (mutable.Map[TopicPartition, MessageSet], mutable.Map[TopicPartition, PartitionResponse]) = {
    val messagesPerPartition: mutable.Map[TopicPartition, MessageSet] =
      requestInfo.map {
        case (topicPartition, buffer) => (topicPartition, new ByteBufferMessageSet(buffer))
      }
    (messagesPerPartition, mutable.Map())
  }

  /**
    * Return input data
    *
    * @param responsePartitionData fetched data
    * @param principal             principal (not used)
    * @return fetched data
    */
  override def reEncryptConsumer(responsePartitionData: Seq[(TopicAndPartition, FetchResponsePartitionData)],
                                 principal: KafkaPrincipal):
  Seq[(TopicAndPartition, FetchResponsePartitionData)] = responsePartitionData

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
  */
class ReEncryptionHandlerImpl(zkUtils: ZkUtils,
                              keysRootPath: String,
                              reEncryptionKeysCacheCapacity: Int,
                              granularReEncryptionKeysCacheCapacity: Int,
                              edekCacheCapacity: Int,
                              channelsCacheCapacity: Int,
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
  private val channelsCache = ReEncryptionHandler.makeCache[String, (Channel, Option[StructuredDataAccessor])](
    channelsCacheCapacity,
    ReEncryptionHandler.DEFAULT_TTL,
    (k: String) => {
      val channel = zkHandler.getChannel(k)
      if (channel == null) null
      else {
        val accessor =
          if (channel.getType == EncryptionType.GRANULAR) {
            val dataAccessor = channel.getAccessorClass.newInstance()
            dataAccessor.configure(props, false)
            Some(dataAccessor)
          } else {
            None
          }
        (channel, accessor)
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
  def isTopicEncrypted(topic: String): Boolean = channelsCache.get(topic).isDefined

  /**
    * Checks rights for re-encryption messages
    *
    * @param topic      topic name to check
    * @param principal  user to check
    * @param clientType client type to check
    * @return result of checking
    */
  def isAllowReEncryption(topic: String, principal: KafkaPrincipal, clientType: ClientType): Boolean = {
    channelsCache(topic)._1._1.getType == EncryptionType.GRANULAR ||
      (reEncryptionKeysCache.get((topic, principal.getName, clientType)) match {
        case Some(key) => !key.isExpired
        case None => false
      })
  }

  /**
    * Re-encrypt all messages from producer
    *
    * @param requestInfo request info
    * @param principal   principal
    * @return re-encrypted data and errors
    */
  def reEncryptProducer(requestInfo: mutable.Map[TopicPartition, ByteBuffer],
                        principal: KafkaPrincipal):
  (mutable.Map[TopicPartition, MessageSet], mutable.Map[TopicPartition, PartitionResponse]) = {
    debug(s"Re-encrypt all messages for producer '${principal.getName}'")
    val reEncrypted = new mutable.HashMap[TopicPartition, MessageSet]
    val errors = new mutable.HashMap[TopicPartition, PartitionResponse]

    for {(topicPartition, bytes) <- requestInfo} {
      val messageSet = new ByteBufferMessageSet(bytes)
      if (messageSet.isEmpty || !isTopicEncrypted(topicPartition.topic())) {
        debug(s"${messageSet.size} messages to the topic '${topicPartition.topic()}' without encryption")
        reEncrypted += topicPartition -> messageSet
      } else if (!isAllowReEncryption(topicPartition.topic(), principal, ClientType.PRODUCER)) {
        error(s"Producer '${principal.getName}' not authorized for writing " +
          s"messages to the topic '${topicPartition.topic()}'")
        errors += topicPartition -> new PartitionResponse(Errors.UNKNOWN.code, -1, Message.NoTimestamp)
      } else Try(reEncryptTopic(topicPartition.topic, principal.getName, ClientType.PRODUCER, messageSet)) match {
        case Success(x) => reEncrypted += topicPartition -> x
        case Failure(e) => error(s"Error while re-encryption topic-partition " +
          s"${topicPartition.topic()}-${topicPartition.partition()}", e)
          errors += topicPartition -> new PartitionResponse(Errors.UNKNOWN.code, -1, Message.NoTimestamp)
      }
    }

    (reEncrypted, errors)
  }

  /**
    * Re-encrypt all messages for consumer
    *
    * @param responsePartitionData fetched data
    * @param principal             principal
    * @return re-encrypted data
    */
  def reEncryptConsumer(responsePartitionData: Seq[(TopicAndPartition, FetchResponsePartitionData)],
                        principal: KafkaPrincipal): Seq[(TopicAndPartition, FetchResponsePartitionData)] = {
    debug(s"Re-encrypt all messages for consumer '${principal.getName}'")
    responsePartitionData.map { case (topicPartition, data) =>
      if (data.messages.isEmpty || !isTopicEncrypted(topicPartition.topic)) {
        debug(s"${data.messages.size} messages from the topic '${topicPartition.topic}' without encryption")
        topicPartition -> data
      } else if (!isAllowReEncryption(topicPartition.topic, principal, ClientType.CONSUMER)) {
        error(s"Consumer '${principal.getName}' not authorized for fetching " +
          s"messages from the topic '${topicPartition.topic}'")
        topicPartition -> FetchResponsePartitionData(Errors.UNKNOWN.code, -1, MessageSet.Empty)
      } else Try(reEncryptTopic(topicPartition.topic, principal.getName, ClientType.CONSUMER, data.messages)) match {
        case Success(x) => topicPartition -> new FetchResponsePartitionData(data.error, data.hw, x)
        case Failure(e) => error(s"Error while re-encryption topic-partition " +
          s"${topicPartition.topic}-${topicPartition.partition}", e)
          topicPartition -> FetchResponsePartitionData(Errors.UNKNOWN.code, -1, MessageSet.Empty)
      }
    }
  }

  /**
    * Re-encrypt all messages in one topic
    *
    * @param topic         topic name
    * @param principalName user principal name
    * @param clientType    client type
    * @param messageSet    messages
    * @return re-encrypted messages
    */
  def reEncryptTopic(topic: String,
                     principalName: String,
                     clientType: ClientType,
                     messageSet: MessageSet): MessageSet = {
    val channel = channelsCache(topic)._1
    channel._1.getType match {
      case EncryptionType.GRANULAR =>
        granularReEncryptTopic(
          channel._1, channel._2.get, principalName, clientType, messageSet)
      case EncryptionType.FULL =>
        fullReEncryptTopic(channel._1, principalName, clientType, messageSet)
    }
  }

  /**
    * Granular re-encrypt all messages in one topic by fields
    *
    * @param channel       channel object
    * @param accessor      data accessor
    * @param principalName user principal name
    * @param clientType    client type
    * @param messageSet    messages
    * @return re-encrypted messages
    */
  def granularReEncryptTopic(channel: Channel,
                             accessor: StructuredDataAccessor,
                             principalName: String,
                             clientType: ClientType,
                             messageSet: MessageSet): MessageSet = {
    reEncryptTopic(
      bytes => reEncryptPayload(bytes, channel, accessor, principalName, clientType),
      messageSet
    )
  }

  /**
    * Full re-encrypt all messages in one topic
    *
    * @param channel       channel object
    * @param principalName user principal name
    * @param clientType    client type
    * @param messageSet    messages
    * @return re-encrypted messages
    */
  def fullReEncryptTopic(channel: Channel,
                         principalName: String,
                         clientType: ClientType,
                         messageSet: MessageSet): MessageSet = {
    val topic = channel.getName
    val keyHolder = reEncryptionKeysCache((topic, principalName, clientType))
    if (keyHolder.getKey.isEmpty) {
      debug(s"${messageSet.size} messages in the topic '$topic' " +
        s"for ${clientType.toString.toLowerCase} " +
        s"'$principalName' are not re-encrypted")
      messageSet
    } else {
      debug(s"Re-encrypt ${messageSet.size} messages in the topic '$topic' " +
        s"for ${clientType.toString.toLowerCase} '$principalName'")
      reEncryptTopic(
        bytes => reEncryptPayload(bytes, keyHolder.getKey),
        messageSet)
    }
  }

  /**
    * Re-encrypt all messages in one topic
    *
    * @param reEncryptFunction re-encryption function
    * @param messageSet        messages
    * @return re-encrypted messages
    */
  def reEncryptTopic(reEncryptFunction: Array[Byte] => Array[Byte],
                     messageSet: MessageSet): MessageSet = {
    val offsets = new ArrayBuffer[Long]
    val newMessages = new ArrayBuffer[Message]

    if (messageSet.isInstanceOf[FileMessageSet]) messageSet.foreach {
      messageAndOffset =>
        val message = messageAndOffset.message
        if (message.compressionCodec == NoCompressionCodec) {
          newMessages += reEncryptMessage(reEncryptFunction, message)
          offsets += messageAndOffset.offset
        } else {
          // File message set only has shallow iterator. We need to do deep iteration here if needed.
          val deepIter = ByteBufferMessageSet.deepIterator(messageAndOffset)
          for (innerMessageAndOffset <- deepIter) {
            newMessages += reEncryptMessage(reEncryptFunction, innerMessageAndOffset.message)
            offsets += innerMessageAndOffset.offset
          }
        }
    }
    else messageSet.foreach {
      messageAndOffset => {
        newMessages += reEncryptMessage(reEncryptFunction, messageAndOffset.message)
        offsets += messageAndOffset.offset
      }
    }

    new ByteBufferMessageSet(
      compressionCodec = messageSet.headOption.map(_.message.compressionCodec).getOrElse(NoCompressionCodec),
      offsetSeq = offsets,
      newMessages: _*)
  }

  /**
    * Re-encrypt one message
    *
    * @param reEncryptFunction re-encryption function
    * @param message           message
    * @return re-encrypted payload
    */
  def reEncryptMessage(reEncryptFunction: Array[Byte] => Array[Byte],
                       message: Message): Message = {
    val payload = message.payload
    var valueBytes: Array[Byte] = null
    if (payload != null) {
      valueBytes = new Array[Byte](payload.remaining)
      payload.get(valueBytes)
      valueBytes = reEncryptFunction(valueBytes)
    }

    val key = message.key
    var keyBytes: Array[Byte] = null
    if (key != null) {
      keyBytes = new Array[Byte](key.remaining)
      key.get(keyBytes)
    }

    new Message(
      valueBytes,
      keyBytes,
      message.timestamp,
      message.timestampType,
      message.compressionCodec,
      0,
      -1,
      message.magic
    )
  }

  /**
    * Full re-encrypt payload from message
    *
    * @param payload message payload
    * @param reKey   re-encryption key
    * @return re-encrypted payload
    */
  def reEncryptPayload(payload: Array[Byte], reKey: WrapperReEncryptionKey): Array[Byte] = {
    messageHandler.reEncrypt(payload, reKey)
  }

  /**
    * Granular re-encrypt payload from message
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

    val structuredMessageHandler = new StructuredMessageHandler(messageHandler)
    val reKeys = structuredMessageHandler
      .getAllEncrypted(channel.getName, payload, accessor).asScala.flatMap {
      case (field) =>
        val reKey = granularReEncryptionKeysCache.get(
          (channel.getName, principalName, clientType, field))
        if (reKey.isEmpty && clientType == ClientType.PRODUCER) {
          throw new CommonException(s"There are no re-encryption key " +
            s"for ${clientType.toString.toLowerCase} '$principalName', " +
            s"channel '${channel.getName}' and field '$field'")
        }
        if (reKey.isEmpty || reKey.get.getKey.isEmpty) {
          //TODO change to trace
          debug(s"Field '$field' in the message in the topic '${channel.getName}' " +
            s"for ${clientType.toString.toLowerCase} '$principalName' is not re-encrypted")
          None
        } else {
          Some(field, reKey.get.getKey)
        }
    }.toMap.asJava
    structuredMessageHandler.reEncrypt(reKeys)
  }

}