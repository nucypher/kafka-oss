package unit.kafka.nucypher

import java.util.concurrent.TimeUnit

import kafka.nucypher.ReEncryptionHandler
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Test

/**
  * Tests for caches in [[kafka.nucypher.ReEncryptionHandler]]
  */
class CacheTest {

  @Test
  def testLRUCache(): Unit = {
    val cache = ReEncryptionHandler.makeCache[String, String](2, (k: String) => k)
    assertEquals(Some("1"), cache.get("1"))
    assertEquals(Some("2"), cache.get("2"))
    assertEquals(Some("3"), cache.get("3"))
    assertEquals(Set("2", "3"), cache.keySet)
  }

  @Test
  def testLRUWithFilteringCache(): Unit = {
    val cache = ReEncryptionHandler.makeCache[String, String](
      2, (v: String) => v.toInt % 2 == 0, (k: String) => k)
    assertEquals(Some("1"), cache.get("1"))
    assertEquals(None, cache.get("2"))
    assertEquals(Some("3"), cache.get("3"))
    assertEquals(None, cache.get("4"))
    assertEquals(Set("1", "3"), cache.keySet)
  }

  @Test
  def testTTLCache(): Unit = {
    var cache = ReEncryptionHandler.makeCache[String, String](10, 2000, (k: String) => k)
    assertEquals("1", cache("1")._1)
    assertEquals("2", cache("2")._1)
    assertEquals("3", cache("3")._1)
    assertEquals(Set("1", "2", "3"), cache.keySet)

    TimeUnit.SECONDS.sleep(1)
    assertEquals("1", cache("1")._1)
    assertEquals("2", cache("2")._1)
    assertEquals("3", cache("3")._1)
    assertEquals(Set("1", "2", "3"), cache.keySet)

    TimeUnit.SECONDS.sleep(1)
    assertEquals(None, cache.get("1"))
    assertEquals(None, cache.get("2"))
    assertEquals(None, cache.get("3"))
    assertEquals(Set(), cache.keySet)

    cache = ReEncryptionHandler.makeCache[String, String](10, 2000, (_: String) => null)
    assertFalse(cache.get("1").isDefined)
  }

}
