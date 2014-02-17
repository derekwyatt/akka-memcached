package com.codeseq.mcdc

import akka.io.{PipelineContext, PipelinePorts, PipelineFactory}
import akka.util.ByteString
import org.scalatest.{WordSpec, Matchers}
import scala.concurrent.duration._

class protocolSpec extends WordSpec with Matchers with Implicits {
  import MemcacheProtocol._

  "marshalling" should { //{1
    "transform a Get object to a Request" in { //{2
      validateGetRequest(0, get.toRequest)
    } //}2
    "transform a GetQ object to a Request" in { //{2
      validateGetRequest(0x09, getq.toRequest)
    } //}2
    "transform a GetK object to a Request" in { //{2
      validateGetRequest(0x0c, getk.toRequest)
    } //}2
    "transform a GetKQ object to a Request" in { //{2
      validateGetRequest(0x0d, getkq.toRequest)
    } //}2
    "transform an Add object to a Request" in { //{2
      validateAddRequest(0x02, add.toRequest)
    } //}2
    "transform an AddQ object to a Request" in { //{2
      validateAddRequest(0x12, addq.toRequest)
    } //}2
    "transform a Set object to a Request" in { //{2
      validateSetRequest(0x01, set.toRequest)
    } //}2
    "transform a SetQ object to a Request" in { //{2
      validateSetRequest(0x11, setq.toRequest)
    } //}2
    "transform a Delete object to a Request" in { //{2
      validateDeleteRequest(0x04, delete.toRequest)
    } //}2
    "transform a DeleteQ object to a Request" in { //{2
      validateDeleteRequest(0x14, deleteq.toRequest)
    } //}2
    "transform a Replace object to a Request" in { //{2
      validateReplaceRequest(0x03, replace.toRequest)
    } //}2
    "transform a ReplaceQ object to a Request" in { //{2
      validateReplaceRequest(0x13, replaceq.toRequest)
    } //}2
    "transform a Quit object to a Request" in { //{2
      validateEmptyRequest(0x07, quit.toRequest)
    } //}2
    "transform a QuitQ object to a Request" in { //{2
      validateEmptyRequest(0x17, quitq.toRequest)
    } //}2
    "transform a Noop object to a Request" in { //{2
      validateEmptyRequest(0x0a, noop.toRequest)
    } //}2
    "transform a Version object to a Request" in { //{2
      validateEmptyRequest(0x0b, version.toRequest)
    } //}2
    "properly encode a request to a ByteString" in { //{2
      val bs = get.toRequest.toByteString
      bs should be (getRequest)
    } //}2
    "properly decode a ByteString to a Response" in { //{2
      val expected = Response(ResponseHeader(0, 3, 0, 0, 0, 8, 0, 0),
                              Some("key"), Some(ByteString("value")), None)
      Response.fromByteString(`key/value-response`) should be (expected)
    } //}2
  } //}1

  val ctx = new PipelineContext { }
  val frame = new MemcacheFrame

  "MemcacheFrame" should { //{1
    "extract nothing" in { //{2
      val PipelinePorts(cmd, evt, mgmt) = PipelineFactory.buildFunctionTriple(ctx, frame)
      val (segmented, _) = evt(ByteString.empty)
      segmented should be ('empty)
    } //}2
    "extract something when that's all there is" in { //{2
      val PipelinePorts(cmd, evt, mgmt) = PipelineFactory.buildFunctionTriple(ctx, frame)
      val (segmented, _) = evt(`key/value-response`)
      segmented should have size (1)
    } //}2
    "extract something when there's more but not enough for two" in { //{2
      val PipelinePorts(cmd, evt, mgmt) = PipelineFactory.buildFunctionTriple(ctx, frame)
      val (segmented, _) = evt(`key/value-response` ++ fragment)
      segmented should have size (1)
    }
    "extract two things when there's two there" in { //{2
      val PipelinePorts(cmd, evt, mgmt) = PipelineFactory.buildFunctionTriple(ctx, frame)
      val (segmented, _) = evt(`key/value-response` ++ `key/value-response`)
      segmented should have size (2)
    }
  } //}1

  val message = new MemcacheMessage >> new MemcacheFrame

  "MemcacheMessage" should { //{1
    "create two responses when there's two there" in { //{2
      val PipelinePorts(cmd, evt, mgmt) = PipelineFactory.buildFunctionTriple(ctx, message)
      val (segmented, _) = evt(`key/value-response` ++ `key/value-response`)
      segmented should have size (2)
      segmented foreach { r =>
        r.header.opCode should be (0)
        r.header.keyLength should be (3)
        r.header.extrasLength should be (0)
        r.header.dataType should be (0)
        r.header.status should be (0)
        r.header.totalBodyLength should be (8)
        r.header.opaque should be (0)
        r.header.cas should be (0)
        r.key should be (Some("key"))
        r.value.map(_.utf8String) should be (Some("value"))
        r.extras should be (None)
      }
    }
  } //}1

  val bsValue = ByteString("value")

  val get = Get("key")
  val getq = GetQ("key")
  val getk = GetK("key")
  val getkq = GetKQ("key")

  val add = Add("key", bsValue)
  val addq = AddQ("key", bsValue)

  val set = Set("key", bsValue)
  val setq = SetQ("key", bsValue)

  val replace = Replace("key", bsValue)
  val replaceq = ReplaceQ("key", bsValue)

  val delete = Delete("key")
  val deleteq = DeleteQ("key")

  val quit = Quit()
  val quitq = QuitQ()

  val noop = Noop()

  val version = Version()

  val `key/value-response` = ByteString(0x81.toByte,             // magic
                                        0,                       // opCode
                                        0, 3,                    // key length
                                        0,                       // extras length
                                        0,                       // data type
                                        0, 0,                    // status
                                        0, 0, 0, 8,              // total body length
                                        0, 0, 0, 0,              // opaque
                                        0, 0, 0, 0, 0, 0, 0, 0,  // cas
                                        'k', 'e', 'y',           // key
                                        'v', 'a', 'l', 'u', 'e') // key
  
  val fragment = ByteString(1, 2, 3)

  val getRequest = ByteString(0x80.toByte,            // magic
                              0,                      // opCode
                              0, 3,                   // key length
                              0,                      // extras length
                              0,                      // data type
                              0, 0,                   // vbucket id
                              0, 0, 0, 3,             // total body length
                              0, 0, 0, 0,             // opaque
                              0, 0, 0, 0, 0, 0, 0, 0, // cas
                              'k', 'e', 'y')          // payload

  def validateGetRequest(opCode: Byte, req: Request): Unit = {
    val (h, p) = (req.header, req.payload)
    h.opCode should be (opCode)
    h.keyLength should be (3)
    h.extrasLength should be (0)
    h.vBucketId should be (0)
    h.totalBodyLength should be (3)
    h.opaque should be (0)
    h.cas should be (0)
    p should be (ByteString("key"))
  }

  def validateAddRequest(opCode: Byte, req: Request): Unit = {
    val (h, p) = (req.header, req.payload)
    h.opCode should be (opCode)
    h.keyLength should be (3)
    h.extrasLength should be (8)
    h.vBucketId should be (0)
    h.totalBodyLength should be (16)
    h.opaque should be (0)
    h.cas should be (0)
    p should be (extras(1.hour) ++ "key" ++ "value")
  }

  def validateSetRequest(opCode: Byte, req: Request): Unit = {
    val (h, p) = (req.header, req.payload)
    h.opCode should be (opCode)
    h.keyLength should be (3)
    h.extrasLength should be (8)
    h.vBucketId should be (0)
    h.totalBodyLength should be (16)
    h.opaque should be (0)
    h.cas should be (0)
    p should be (extras(1.hour) ++ "key" ++ "value")
  }

  def validateDeleteRequest(opCode: Byte, req: Request): Unit = {
    val (h, p) = (req.header, req.payload)
    h.opCode should be (opCode)
    h.keyLength should be (3)
    h.extrasLength should be (0)
    h.vBucketId should be (0)
    h.totalBodyLength should be (3)
    h.opaque should be (0)
    h.cas should be (0)
    p should be (ByteString("key"))
  }

  def validateReplaceRequest(opCode: Byte, req: Request): Unit = {
    val (h, p) = (req.header, req.payload)
    h.opCode should be (opCode)
    h.keyLength should be (3)
    h.extrasLength should be (8)
    h.vBucketId should be (0)
    h.totalBodyLength should be (16)
    h.opaque should be (0)
    h.cas should be (0)
    p should be (extras(1.hour) ++ "key" ++ "value")
  }

  def validateEmptyRequest(opCode: Byte, req: Request): Unit = {
    val (h, p) = (req.header, req.payload)
    h.opCode should be (opCode)
    h.keyLength should be (0)
    h.extrasLength should be (0)
    h.vBucketId should be (0)
    h.totalBodyLength should be (0)
    h.opaque should be (0)
    h.cas should be (0)
    p should be (ByteString.empty)
  }
}
// vim:fdl=1:
