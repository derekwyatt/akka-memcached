package com.codeseq.mcdc

import org.scalatest.{WordSpec, Matchers}
import scala.concurrent.duration._

class clientSpec extends WordSpec with Matchers {
  import MemcachedClient._

  "jittered" should { //{1
    "return a non-jittered duration" in { //{2
      jittered(TTL(60.hours), NoJitter) should be (60.hours)
    } //}2
    "return a jittered duration" in { //{2
      val secs = 60.hours.toSeconds
      val plus12 = (60.hours + 12.hours).toSeconds
      val minus12 = (60.hours - 12.hours).toSeconds
      var gt0 = 0
      var gt12 = 0
      var lt0 = 0
      var lt12 = 0
      val results = for (_ <- 1 to 10000) {
        val jit = (jittered(TTL(60.hours), TTLJitterFactor(0.8f))).toSeconds
        if (jit > plus12) gt12 += 1
        else if (jit > secs) gt0 += 1
        else if (jit < minus12) lt12 += 1
        else if (jit < secs) lt0 += 1
      }
      gt0  should be >= (1000)
      gt12 should be >= (1000)
      lt0  should be >= (1000)
      lt12 should be >= (1000)
    } //}2
  } //}1
}
// vim:fdl=1:
