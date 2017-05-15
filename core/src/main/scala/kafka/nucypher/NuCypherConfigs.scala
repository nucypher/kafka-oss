package kafka.nucypher

import org.apache.zookeeper.common.PathUtils

import scala.util.{Failure, Success, Try}

/**
  * Class with NuCypher configuration parameters
  */
object NuCypherConfigs {
  val NUCYPHER_RE_ENCRYPTION_KEYS_PATH = "nucypher.reencryption.keys.path"
  val NUCYPHER_RE_ENCRYPTION_KEYS_PATH_DOC = "Root path in ZooKeeper where re-encryption keys are stored"

  val NUCYPHER_CACHE_RE_ENCRYPTION_KEYS_CAPACITY = "nucypher.cache.reencryption.keys.capacity"
  val NUCYPHER_CACHE_RE_ENCRYPTION_KEYS_CAPACITY_DOC = "Re-encryption keys cache capacity"
  val NUCYPHER_CACHE_RE_ENCRYPTION_KEYS_CAPACITY_DEFAULT = 100000

  val NUCYPHER_CACHE_GRANULAR_RE_ENCRYPTION_KEYS_CAPACITY = "nucypher.cache.reencryption.keys.granular.capacity"
  val NUCYPHER_CACHE_GRANULAR_RE_ENCRYPTION_KEYS_CAPACITY_DOC = "Granular re-encryption keys cache capacity"
  val NUCYPHER_CACHE_GRANULAR_RE_ENCRYPTION_KEYS_CAPACITY_DEFAULT = 1000000

  val NUCYPHER_CACHE_EDEK_CAPACITY = "nucypher.cache.edek.capacity"
  val NUCYPHER_CACHE_EDEK_CAPACITY_DOC = "EDEK cache capacity"
  val NUCYPHER_CACHE_EDEK_CAPACITY_DEFAULT = NUCYPHER_CACHE_RE_ENCRYPTION_KEYS_CAPACITY_DEFAULT

  val NUCYPHER_CACHE_CHANNELS_CAPACITY = "nucypher.cache.channels.capacity"
  val NUCYPHER_CACHE_CHANNELS_CAPACITY_DOC = "Channels cache capacity"
  val NUCYPHER_CACHE_CHANNELS_CAPACITY_DEFAULT = 1000

  def validatePath(keysRootPath: String): Boolean = {
    Try(PathUtils.validatePath(keysRootPath)) match {
      case Success(_) => true
      case Failure(_) => false
    }
  }
}
