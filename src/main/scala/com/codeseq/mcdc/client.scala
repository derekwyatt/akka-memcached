package com.codeseq.mcdc

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.{ByteString, Timeout}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object MemcachedClient {
  case class TTL(duration: Duration)

  sealed trait TTLJitter
  case class TTLJitterFactor(factor: Float) extends TTLJitter
  case object NoJitter extends TTLJitter

  def jittered(ttl: TTL, jitter: TTLJitter): Duration = jitter match {
    case TTLJitterFactor(factor) =>
      val max = (ttl.duration.toSeconds * factor).toInt
      val mid = max / 2
      val jitterSeconds = scala.util.Random.nextInt(max)
      val delta = jitterSeconds - mid
      (ttl.duration + delta.seconds).max(0.seconds)
    case NoJitter =>
      ttl.duration
  }

  implicit val settingTTL = TTL(1.hour)

  val NoError                 = 0x00
  val KeyNotFound             = 0x01
  val KeyExists               = 0x02
  val ValueTooLarge           = 0x03
  val InvalidArguments        = 0x04
  val ItemNotStored           = 0x05
  val NonNumericValue         = 0x06
  val VBucketBelongsToAnother = 0x07
  val AuthError               = 0x08
  val AuthContinue            = 0x09
  val UnknownCommand          = 0x81
  val OutOfMemory             = 0x82
  val NotSupported            = 0x83
  val InternalError           = 0x84
  val Busy                    = 0x85
  val TemporaryFailure        = 0x86

  case class MemcacheOperationFailed(code: Int, reason: String) extends RuntimeException(reason)
  case class MemcacheOperationAnomaly(reason: String) extends RuntimeException(reason)
}

class MemcachedClient(nodes: Seq[ActorRef]) extends Implicits {
  import MemcachedClient._
  import MemcachedClientActor._
  import MemcacheProtocol._

  require(nodes.size > 0, "MemcachedClient requires at least one node.")

  private val ring = HashRing(nodes, 10)

  private def processRsp[T](rsp: Response)(f: (ResponseHeader, Option[String], Option[ByteString], Option[ByteString]) => T): T =
    rsp match {
      case Response(header, key, value, extras) =>
        if (header.status == 0)
          f(header, key, value, extras)
        else {
          val msg = extras map { _.utf8String } getOrElse ""
          throw new MemcacheOperationFailed(header.status, msg)
        }
      case rsp =>
        throw new MemcacheOperationAnomaly(s"Unknown response: $rsp")
    }

  def send(req: Request)(implicit ec: ExecutionContext, to: Timeout): Future[Response] =
    (nodes.head ? Send(req)).mapTo[Response] map { rsp =>
      processRsp(rsp) { (header, key, value, extras) => Response(header, key, value, extras) }
    }

  def set(key: String, magnet: ByteStringMagnet)(implicit ec: ExecutionContext, to: Timeout, ttl: TTL, ttlJitter: TTLJitter = NoJitter): Future[CAS] =
    magnet { value =>
      val connection = ring.getNodeFor(key.getBytes("UTF-8"))
      (connection ? Send(Set(key, value, jittered(ttl, ttlJitter)).toRequest)).mapTo[Response] map { rsp =>
        processRsp(rsp) { (header, _, _, _) =>
          header.cas
        }
      }
    }

  def get(key: String)(implicit ec: ExecutionContext, to: Timeout): Future[Option[ByteString]] = {
    val connection = ring.getNodeFor(key.getBytes("UTF-8"))
    (connection ? Send(Get(key).toRequest)).mapTo[Response] map { rsp =>
      processRsp(rsp) { (_, _, value, _) =>
        value
      }
    } recover {
      case MemcacheOperationFailed(KeyNotFound, _) =>
        None
    }
  }

  def delete(key: String)(implicit ec: ExecutionContext, to: Timeout): Future[Unit] = {
    val connection = ring.getNodeFor(key.getBytes("UTF-8"))
    (connection ? Send(Delete(key).toRequest)).mapTo[Response] map { rsp =>
      processRsp(rsp) { (_, _, _, _) => () }
    }
  }
}
