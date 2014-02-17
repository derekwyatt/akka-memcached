package com.codeseq.mcdc

import akka.io.{PipelineContext, SymmetricPipePair, SymmetricPipelineStage}
import akka.util.{ByteString, ByteIterator}
import scala.annotation.{implicitNotFound, tailrec}
import scala.concurrent.duration._

class MarshallingException(reason: String) extends RuntimeException(reason)

case class Get(key: String)
case class GetQ(key: String)
case class GetK(key: String)
case class GetKQ(key: String)
case class Add(key: String, value: ByteString, ttl: Duration = 1.hour)
case class AddQ(key: String, value: ByteString, ttl: Duration = 1.hour)
case class Set(key: String, value: ByteString, ttl: Duration = 1.hour)
case class SetQ(key: String, value: ByteString, ttl: Duration = 1.hour)
case class Replace(key: String, value: ByteString, ttl: Duration = 1.hour)
case class ReplaceQ(key: String, value: ByteString, ttl: Duration = 1.hour)
case class Delete(key: String)
case class DeleteQ(key: String)
case class Quit()
case class QuitQ()
case class Noop()
case class Version()

// Not yet implemented
case class Append(key: String, value: ByteString)
case class AppendQ(key: String, value: ByteString)
case class Increment(key: String, initial: Long, delta: Long, ttl: Duration = 1.hour)
case class IncrementQ(key: String, initial: Long, delta: Long, ttl: Duration = 1.hour)
case class Decrement(key: String, initial: Long, delta: Long, ttl: Duration = 1.hour)
case class DecrementQ(key: String, initial: Long, delta: Long, ttl: Duration = 1.hour)
case class Flush(atTime: Duration = Duration.Inf)
case class GetAndTouch(key: String, ttl: Duration = 1.hour)
case class GetAndTouchQ(key: String, ttl: Duration = 1.hour)
case class Prepend(key: String, value: ByteString)
case class PrependQ(key: String, value: ByteString)
case class Stat(key: Option[String])
case class Touch(key: String, ttl: Duration = 1.hour)

case class RequestHeader(opCode: Byte, keyLength: Short,
                         extrasLength: Byte, dataType: Byte, vBucketId: Short,
                         totalBodyLength: Int, opaque: Int, cas: Long) {
  val magic = 0x80.toByte
  def toByteString(implicit bo: java.nio.ByteOrder): ByteString = {
    val builder = ByteString.newBuilder
    builder.putByte(magic)
    builder.putByte(opCode)
    builder.putShort(keyLength)
    builder.putByte(extrasLength)
    builder.putByte(dataType)
    builder.putShort(vBucketId)
    builder.putInt(totalBodyLength)
    builder.putInt(opaque)
    builder.putLong(cas)
    builder.result
  }
}
case class Request(header: RequestHeader, payload: ByteString) {
  def toByteString(implicit bo: java.nio.ByteOrder): ByteString = header.toByteString ++ payload
}

case class ResponseHeader(opCode: Byte, keyLength: Short,
                          extrasLength: Byte, dataType: Byte, status: Short,
                          totalBodyLength: Int, opaque: Int, cas: Long) {
  val magic = 0x81.toByte
}
object ResponseHeader {
  def fromByteIterator(bi: ByteIterator)(implicit bo: java.nio.ByteOrder): (ResponseHeader, ByteIterator) = {
    val magic = bi.getByte
    if (magic != 0x81.toByte)
      throw new MarshallingException(s"Cannot unmarshall byte stream to ResponseHeader. Magic is not 0x81 ($magic).")
    val opCode = bi.getByte
    val keyLen = bi.getShort
    val extrasLen = bi.getByte
    val dataType = bi.getByte
    val status = bi.getShort
    val totalBodyLen = bi.getInt
    val opaque = bi.getInt
    val cas = bi.getLong
    (ResponseHeader(opCode, keyLen, extrasLen, dataType, status, totalBodyLen, opaque, cas), bi)
  }
}

case class Response(header: ResponseHeader, key: Option[String], value: Option[ByteString], extras: Option[ByteString])

object Response {
  def fromByteString(bs: ByteString)(implicit bo: java.nio.ByteOrder): Response = {
    val (header, bi) = ResponseHeader.fromByteIterator(bs.iterator)
    val payload = bi.toByteString
    if (header.totalBodyLength != payload.size)
      throw new MarshallingException(s"Unable to construct Response. Header's total body length (${header.totalBodyLength}) does not match payload size (${payload.size}).")
    val iter = payload.iterator
    val extras = if (header.extrasLength != 0) {
                   val bytes = Array.fill[Byte](header.extrasLength)(0x0)
                   iter.getBytes(bytes, 0, header.extrasLength)
                   Some(ByteString(bytes))
                 } else
                   None
    val key = if (header.keyLength != 0) {
                val bytes = Array.fill[Byte](header.keyLength)(0x0)
                iter.getBytes(bytes, 0, header.keyLength)
                Some(new String(bytes, "UTF-8"))
              } else
                None
    val valueLength = header.totalBodyLength - header.extrasLength - header.keyLength
    val value = if (valueLength != 0) Some(iter.toByteString) else None
    Response(header, key, value, extras)
  }
}

object MemcacheProtocol {
  class MemcacheFrame extends SymmetricPipelineStage[PipelineContext, ByteString, ByteString] with Implicits {
    def extractDataUnit(bytes: ByteString): (ByteString, ByteString) = {
      if (bytes.size >= 24) {
        val totalBodySize = bytes.iterator.drop(8).getInt
        if ((bytes.size - 24) < totalBodySize) {
          (ByteString.empty, bytes)
        } else {
          val bs = bytes.iterator.take(24 + totalBodySize).toByteString.compact
          val rest = bytes.iterator.drop(24 + totalBodySize).toByteString.compact
          (bs, rest)
        }
      } else {
        (ByteString.empty, bytes)
      }
    }

    def apply(ctx: PipelineContext) = new SymmetricPipePair[ByteString, ByteString] {
      var buffer = None: Option[ByteString]

      @tailrec
      def extractResponseFrames(bs: ByteString, acc: List[ByteString]): (Option[ByteString], Seq[ByteString]) = {
        if (bs.isEmpty) {
          (None, acc)
        } else if (bs.length < 24) {
          (Some(bs.compact), acc)
        } else {
          val (pdu, extra) = extractDataUnit(bs)
          if (pdu.isEmpty) {
            (Some(bs.compact), acc)
          } else {
            extractResponseFrames(extra, pdu :: acc)
          }
        }
      }

      def commandPipeline = { bs: ByteString =>
        throw new RuntimeException("Command Pipeline for frames isn't supported.")
      }

      def eventPipeline = { bs: ByteString =>
        val data = if (buffer.isEmpty) bs else buffer.get ++ bs
        val (nb, frames) = extractResponseFrames(data, Nil)
        buffer = nb
        frames match {
          case Nil => Nil
          case one :: Nil => ctx.singleEvent(one)
          case many => many reverseMap (Left(_))
        }
      }
    }
  }

  class MemcacheMessage extends SymmetricPipelineStage[PipelineContext, Response, ByteString] with Implicits {
    def apply(ctx: PipelineContext) = new SymmetricPipePair[Response, ByteString] {
      val commandPipeline = { _: Response =>
        throw new RuntimeException("Command Pipeline for responses isn't supported.")
      }
      val eventPipeline = { bs: ByteString =>
        ctx.singleEvent(Response.fromByteString(bs))
      }
    }
  }

  @implicitNotFound("Can't marshall the given value due to a lack of an implicit PDUWriter.")
  trait PDUWriter[T] {
    def write(t: T)(implicit bo: java.nio.ByteOrder): Request
  }

  def transformRequest(operationCode: Int,
                       key: Option[String] = None,
                       value: Option[ByteString] = None,
                       extras: Option[ByteString] = None): Request = {
    val keyLen = key map { _.size } getOrElse 0
    val extraLen = extras map { _.size } getOrElse 0
    val valueLen = value map { _.size } getOrElse 0
    val totalBodyLen = keyLen + extraLen + valueLen

    val builder = ByteString.newBuilder
    extras foreach { builder ++= _ }
    key foreach { builder ++= _.getBytes("UTF-8") }
    value foreach { builder ++= _ }

    Request(RequestHeader(opCode = operationCode.toByte,
                          keyLength = keyLen.toShort,
                          extrasLength = extraLen.toByte,
                          dataType = 0x0,
                          vBucketId = 0x0,
                          totalBodyLength = totalBodyLen,
                          opaque = 0x0,
                          cas = 0x0),
            builder.result())
  }

  def extras(ttl: Duration)(implicit bo: java.nio.ByteOrder): ByteString = {
    val builder = ByteString.newBuilder
    builder.putInt(0xdeadbeef)
    builder.putInt(ttl.toSeconds.toInt)
    builder.result
  }

  implicit object getProtocol extends PDUWriter[Get] {
    def write(get: Get)(implicit bo: java.nio.ByteOrder): Request =
      transformRequest(operationCode = 0x00, key = Some(get.key))
  }

  implicit object getqProtocol extends PDUWriter[GetQ] {
    def write(get: GetQ)(implicit bo: java.nio.ByteOrder): Request =
      transformRequest(operationCode = 0x09, key = Some(get.key))
  }

  implicit object getkProtocol extends PDUWriter[GetK] {
    def write(get: GetK)(implicit bo: java.nio.ByteOrder): Request =
      transformRequest(operationCode = 0x0c, key = Some(get.key))
  }

  implicit object getkqProtocol extends PDUWriter[GetKQ] {
    def write(get: GetKQ)(implicit bo: java.nio.ByteOrder): Request =
      transformRequest(operationCode = 0x0d, key = Some(get.key))
  }

  implicit object setProtocol extends PDUWriter[Set] {
    def write(set: Set)(implicit bo: java.nio.ByteOrder): Request =
      transformRequest(operationCode = 0x01, key = Some(set.key), value = Some(set.value), extras = Some(extras(set.ttl)))
  }

  implicit object setqProtocol extends PDUWriter[SetQ] {
    def write(set: SetQ)(implicit bo: java.nio.ByteOrder): Request =
      transformRequest(operationCode = 0x11, key = Some(set.key), value = Some(set.value), extras = Some(extras(set.ttl)))
  }

  implicit object replaceProtocol extends PDUWriter[Replace] {
    def write(replace: Replace)(implicit bo: java.nio.ByteOrder): Request =
      transformRequest(operationCode = 0x03, key = Some(replace.key), value = Some(replace.value), Some(extras(replace.ttl)))
  }

  implicit object replaceqProtocol extends PDUWriter[ReplaceQ] {
    def write(replace: ReplaceQ)(implicit bo: java.nio.ByteOrder): Request =
      transformRequest(operationCode = 0x13, key = Some(replace.key), value = Some(replace.value), Some(extras(replace.ttl)))
  }

  implicit object deleteProtocol extends PDUWriter[Delete] {
    def write(delete: Delete)(implicit bo: java.nio.ByteOrder): Request = transformRequest(operationCode = 0x04, key = Some(delete.key))
  }

  implicit object deleteqProtocol extends PDUWriter[DeleteQ] {
    def write(delete: DeleteQ)(implicit bo: java.nio.ByteOrder): Request = transformRequest(operationCode = 0x14, key = Some(delete.key))
  }

  implicit object addProtocol extends PDUWriter[Add] {
    def write(add: Add)(implicit bo: java.nio.ByteOrder): Request =
      transformRequest(operationCode = 0x02, key = Some(add.key), value = Some(add.value), extras = Some(extras(add.ttl)))
  }

  implicit object addqProtocol extends PDUWriter[AddQ] {
    def write(add: AddQ)(implicit bo: java.nio.ByteOrder): Request =
      transformRequest(operationCode = 0x12, key = Some(add.key), value = Some(add.value), extras = Some(extras(add.ttl)))
  }

  implicit object noopProtocol extends PDUWriter[Noop] {
    def write(noop: Noop)(implicit bo: java.nio.ByteOrder): Request = transformRequest(operationCode = 0x0a)
  }

  implicit object quitProtocol extends PDUWriter[Quit] {
    def write(quit: Quit)(implicit bo: java.nio.ByteOrder): Request = transformRequest(operationCode = 0x07)
  }

  implicit object quitqProtocol extends PDUWriter[QuitQ] {
    def write(quitq: QuitQ)(implicit bo: java.nio.ByteOrder): Request = transformRequest(operationCode = 0x17)
  }

  implicit object versionProtocol extends PDUWriter[Version] {
    def write(version: Version)(implicit bo: java.nio.ByteOrder): Request = transformRequest(operationCode = 0x0b)
  }

  implicit class ProtocolWritable[T : PDUWriter](t: T) {
    def toRequest(implicit bo: java.nio.ByteOrder): Request = implicitly[PDUWriter[T]].write(t)
  }
}
