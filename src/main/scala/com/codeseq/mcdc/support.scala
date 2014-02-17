package com.codeseq.mcdc

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.io.Tcp
import akka.util.ByteString
import scala.collection.{Iterable, Iterator}
import scala.collection.mutable

class WriteThrottler(connection: ActorRef) extends Actor with ActorLogging {
  import Tcp._

  var buffer = mutable.Queue.empty[ByteString]
  var writeable = true

  object WriteAck extends Event

  def flush(): Unit = {
    if (writeable) {
      if (!buffer.isEmpty) {
        val data = buffer.take(20).reduce(_ ++ _)
        buffer = buffer drop 20
        connection ! Write(data, WriteAck)
        writeable = false
      }
    }
  }

  def writeOutbound(bytes: ByteString): Unit = {
    buffer += bytes
    writeable = true
    flush()
  }

  def receive = {
    case bytes: ByteString =>
      writeOutbound(bytes)
    case WriteAck =>
      writeable = true
      flush()
  }
}

object WriteThrottler {
  def props(connection: ActorRef): Props = Props(new WriteThrottler(connection))
}

class CircularIterator[A](iterable: => Iterable[A]) extends Iterator[A] {
  var it = iterable.iterator
  var idx = 0

  def hasNext: Boolean = it.hasNext

  def next(): A = {
    if (!hasNext) it = {
      idx = -1
      iterable.iterator
    }
    idx += 1
    it.next()
  }

  def nextNoCycle(p: A => Boolean): Option[A] = {
    val start = idx
    var n = next()
    while (!p(n) && (idx != start)) {
      n = next()
    }
    if (!p(n)) None else Some(n)
  }
}

object CircularIterator {
  def apply[A](iter: => Iterable[A]): CircularIterator[A] = new CircularIterator(iter)
}
