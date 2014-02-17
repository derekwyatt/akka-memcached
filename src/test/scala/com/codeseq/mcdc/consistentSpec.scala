package com.codeseq.mcdc

import org.scalatest.{WordSpec, Matchers}

class consistentSpec extends WordSpec with Matchers {
  case class Node(host: String, port: Int)
  def nodes(num: Int) = for (i <- 1 to num) yield Node(s"host$i", i)
  def keys(prefix: String, num: Int): Seq[Array[Byte]] = for (i <- 1 to num) yield s"$prefix:$i".getBytes("UTF-8")

  "consistent" should { //{1
    "throw an IllegalStateException on empty" in { //{2
      val hr = HashRing(Nil, 10)
      an[IllegalStateException] should be thrownBy hr.getNodeFor("hi".getBytes)
    } //}2
    "add nodes properly" in { //{2
      val ns = nodes(2)
      val hr = HashRing(ns.slice(0, 1), 1)
      hr.ring.size should be (1)
      hr.ring.head._2 should be (ns(0))
      val hr2 = hr.addNode(ns(1))
      hr2.ring.size should be (2)
      hr2.ring.values.toSeq should contain (ns(0))
      hr2.ring.values.toSeq should contain (ns(1))
    } //}2
    "remove nodes properly" in { //{2
      val ns = nodes(5)
      val hr = HashRing(ns, 1)
      hr.ring.size should be (5)
      hr.ring.values.toSeq should contain (ns(0))
      val hr2 = hr.removeNode(ns(0))
      hr2.ring.size should be (4)
      hr2.ring.values.toSeq should not contain (ns(0))
    } //}2
    "initialize with about 100 instances for 10 & 10" in { //{2
      val hr = HashRing(nodes(10), 10)
      hr.ring.size should be > (95)
    } //}2
  } //}1
}
// vim:fdl=1:
