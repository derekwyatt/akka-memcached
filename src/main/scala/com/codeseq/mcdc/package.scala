package com.codeseq

import akka.util.ByteString
import scala.concurrent.Future

package object mcdc {
  type CAS = Long

  sealed trait ByteStringMagnet {
    val bs: ByteString
    def apply(f: ByteString => Future[CAS]): Future[CAS] = f(bs)
  }
  
  object ByteStringMagnet {
    import language.implicitConversions
    implicit def fromString(s: String): ByteStringMagnet = new ByteStringMagnet {
      val bs = ByteString(s)
    }
    implicit def fromByteString(bytes: ByteString): ByteStringMagnet = new ByteStringMagnet {
      val bs = bytes
    }
  }
}
