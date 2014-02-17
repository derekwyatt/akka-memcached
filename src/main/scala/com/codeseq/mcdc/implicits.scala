package com.codeseq.mcdc

import java.nio.ByteOrder

trait Implicits {
  implicit val networkByteOrder = ByteOrder.BIG_ENDIAN
}
