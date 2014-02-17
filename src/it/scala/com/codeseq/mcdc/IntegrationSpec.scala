package com.codeseq.mcdc

import akka.actor.{ActorSystem, Props, Actor, ActorRef}
import akka.pattern.ask
import akka.testkit.{TestKit, TestProbe, ImplicitSender}
import akka.util.{ByteString, Timeout}
import java.io.DataInputStream
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import scala.concurrent.duration._
import scala.util.Failure

class IntegrationSpec extends TestKit(ActorSystem("IntegrationSpec"))
                         with ImplicitSender
                         with WordSpecLike
                         with BeforeAndAfterAll
                         with Matchers
                         with Implicits {

  import system.dispatcher
  import MemcacheProtocol._
  import MemcachedClient._

  case class InvalidCommand()
  implicit object invalidCommandProtocol extends PDUWriter[InvalidCommand] {
    def write(invalid: InvalidCommand)(implicit bo: java.nio.ByteOrder): Request = transformRequest(operationCode = 0xff)
  }

  implicit val askTimeout = Timeout(5.seconds)
  implicit val atMostDuration = 5.seconds

  override def afterAll() {
    system.shutdown()
  }

  val image = Array.fill[Byte](47272)(0x0)
  new DataInputStream(getClass.getResourceAsStream("/CodeSeq.png")).readFully(image)

  val gHost = "localhost"
  val gPort = 11211

  trait SUT {
    val probe = TestProbe()
    val actor = system.actorOf(MemcachedClientActor.props(gHost, gPort))
    def client = new MemcachedClient(List(actor))
    actor ! EventPublisher.Register(probe.ref)
    probe.expectMsg(MemcachedClientActor.ConnectionUp(gHost, gPort))
  }

  "Integration" should { //{1
    "run a set/get/delete against a single server" in new SUT { //{2
      client.set("testKey", "testValue").awaitReady
      client.get("testKey").awaitResult.map(_.utf8String) should be (Some("testValue"))
      client.delete("testKey").awaitReady
      client.get("testKey").awaitResult should be (None)
    } //}2
    "set and get something big" in new SUT { //{2
      client.set("testKey", ByteString(image)).awaitReady
      client.get("testKey").awaitResult should be (Some(ByteString(image)))
      client.delete("testKey").awaitReady
      client.get("testKey").awaitResult should be (None)
    } //}2
    "handle an unknown command" in new SUT { //{2
      val futureResult = client.send(InvalidCommand().toRequest)
      futureResult.awaitReady
      futureResult.value should not be ('empty)
      val Some(Failure(MemcacheOperationFailed(code, msg))) = futureResult.value
      code should be (UnknownCommand)
    } //}2
  } //}1
}
// vim:fdl=1:
