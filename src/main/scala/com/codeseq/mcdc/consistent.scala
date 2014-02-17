package com.codeseq.mcdc

import scala.collection.immutable.TreeMap
import scala.util.hashing.MurmurHash3

sealed trait HashRing[T] {
  val nodes: Seq[T]
  val replicas: Int
  val ring: TreeMap[Int, T]
  def addNode(node: T): HashRing[T]
  def removeNode(node: T): HashRing[T]
  def getNodeFor(key: Array[Byte]): T
}

/**
 * Inspired by / ripped off from:
 * https://github.com/debasishg/scala-redis/blob/5422f91bfe8d3156612c0b8967e0fa31ca891952/src/main/scala/com/redis/cluster/HashRing.scala
 */
object HashRing {
  val murmur = MurmurHash3.bytesHashing
  def hashNode[T](node: T, replica: Int): Int = murmur.hash(s"$node:$replica".getBytes("UTF-8"))
  def hashBytes(bytes: Array[Byte]): Int = murmur.hash(bytes)

  private def createRing[T](nodes: Seq[T], replicas: Int): TreeMap[Int, T] = {
    val builder = TreeMap.newBuilder[Int, T]
    for (n <- nodes; i <- 1 to replicas) {
      builder += (hashNode(n, i) -> n)
    }
    builder.result()
  }

  private case class HashRingImpl[T](nodes: Seq[T], replicas: Int, ring: TreeMap[Int, T]) extends HashRing[T] {
    def addNode(node: T): HashRing[T] = {
      val entries = for (i <- 1 to replicas) yield (hashNode(node, i) -> node)
      HashRingImpl(nodes :+ node, replicas, ring ++ entries)
    }

    def removeNode(node: T): HashRing[T] = {
      val keys = ring.filter {
        case (_, v) => v == node
      }.keys
      HashRingImpl(nodes.filterNot(_ == node), replicas, ring -- keys)
    }

    def getNodeFor(key: Array[Byte]): T = {
      if (ring.isEmpty)
        throw new IllegalStateException(s"Can't get node for key ($key) from empty ring.")
      val hash = hashBytes(key)
      def nextClockwise: T = {
        val (ringKey, node) = ring.rangeImpl(Some(hash), None).headOption.getOrElse(ring.head)
        node
      }
      ring.getOrElse(hash, nextClockwise)
    }
  }

  def apply[T](nodes: Seq[T], replicas: Int): HashRing[T] =
    HashRingImpl(nodes, replicas, createRing(nodes, replicas))
}
