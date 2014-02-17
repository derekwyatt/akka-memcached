package com.codeseq.mcdc

import akka.actor.{ActorSystem, Actor, ActorRef, ExtensionId, Props}
import akka.io.IO.Extension
import akka.io.Tcp
import akka.testkit.{TestKit, TestProbe, ImplicitSender}
import java.net.InetSocketAddress
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class ioSpec extends TestKit(ActorSystem("ioSpec")) with ImplicitSender
                                                    with WordSpecLike
                                                    with BeforeAndAfterAll
                                                    with Matchers {
  import MemcachedClientActor._

  val gHost = "host"
  val gPort = 11211
  val remoteAddr = new InetSocketAddress(gHost, gPort)
  val localAddr = new InetSocketAddress("localhost", 10000)

  trait TestIOProvider extends IOPRovider {
    val ioRef: ActorRef
    def io[T <: Extension](key: ExtensionId[T])(implicit system: ActorSystem): ActorRef = ioRef
  }

  class SUT {
    val probe = TestProbe()
    val ioActor = TestProbe()
    val memcache = system.actorOf(Props(new MemcachedClientActor with TestIOProvider {
      lazy val host = gHost
      lazy val port = gPort
      lazy val ioRef: ActorRef = ioActor.ref
    }))
  }

  override def afterAll() {
    system.shutdown()
  }

  def handleConnect(probe: TestProbe, memcache: ActorRef): Unit = {
    probe.expectMsg(Tcp.Connect(remoteAddr))
    probe.reply(Tcp.Connected(remoteAddr, localAddr))
    probe.expectMsg(Tcp.Register(memcache))
  }

  "MemcachedClientActor" should { //{1
    "ask to connect on startup" in new SUT { //{2
      ioActor.expectMsg(Tcp.Connect(remoteAddr))
    } //}2
    "send a notification after connect" in new SUT { //{2
      memcache ! EventPublisher.Register(probe.ref)
      handleConnect(ioActor, memcache)
      probe.expectMsg(ConnectionUp(gHost, gPort))
    } //}2
    "send a notification even when connected" in new SUT { //{2
      handleConnect(ioActor, memcache)
      memcache ! EventPublisher.Register(probe.ref)
      probe.expectMsg(ConnectionUp(gHost, gPort))
    } //}2
    "send a notification even when connection closes" in new SUT { //{2
      handleConnect(ioActor, memcache)
      memcache ! EventPublisher.Register(probe.ref)
      probe.expectMsg(ConnectionUp(gHost, gPort))
      memcache ! Tcp.Closed
      probe.expectMsg(ConnectionDown(gHost, gPort))
    } //}2
    "try to reconnect on close" in new SUT { //{2
      handleConnect(ioActor, memcache)
      memcache ! Tcp.Closed
      handleConnect(ioActor, memcache)
    } //}2
  } //}1
}
