package com.codeseq.mcdc

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.{ByteString, Timeout}
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

object driver extends App with Implicits {
  import MemcacheProtocol._

  val sys = ActorSystem("MemcachedDriver")
  import sys.dispatcher

  implicit val askTimeout: Timeout = 5.seconds

  val a = sys.actorOf(MemcachedClientActor.props("localhost", 11211), "ClientActor")
  val client = new MemcachedClient(List(a))
  Thread.sleep(1000)

  println(Await.result(client.set("key", ByteString("value")), 5.seconds))
  println(Await.result(client.get("key") map { o => o.map { _.utf8String } }, 5.seconds))

  sys.shutdown()
}
