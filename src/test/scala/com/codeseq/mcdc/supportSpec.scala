package com.codeseq.mcdc

import org.scalatest.{WordSpec, Matchers}

class collectionsSpec extends WordSpec with Matchers {
  val emptyList = Nil
  val list1 = List(1)
  val seq5 = (1 to 5).toSeq

  "collections" should { //{1
    "throw for empty lists" in { //{2
      val it = CircularIterator(emptyList)
      an [Exception] should be thrownBy it.next
    } //}2
    "work for a list of 1" in { //{2
      val it = CircularIterator(list1)
      it.next should be (1)
      it.next should be (1)
      it.next should be (1)
    } //}2
    "work for a seq of 5" in { //{2
      val it = CircularIterator(seq5)
      for (_ <- 1 to 10) {
        it.next should be (1)
        it.next should be (2)
        it.next should be (3)
        it.next should be (4)
        it.next should be (5)
      }
    } //}2
    "return Some(3)" in { //{2
      val it = CircularIterator(seq5)
      it.nextNoCycle(a => a == 3) should be (Some(3))
    } //}2
    "return None when starting from 0" in { //{2
      val it = CircularIterator(seq5)
      it.nextNoCycle(a => a == 12) should be (None)
    } //}2
    "return None when starting from 2" in { //{2
      val it = CircularIterator(seq5)
      it.next()
      it.next()
      it.next()
      it.nextNoCycle(a => a == 12) should be (None)
    } //}2
  } //}1
}
// vim:fdl=1:
