package com.codeseq

import scala.concurrent.{Await, Awaitable}
import scala.concurrent.duration._

package object mcdc {
  implicit class AwaitablePimp[T](val awaitable: Awaitable[T]) {
    def awaitReady(implicit atMost: Duration): awaitable.type = Await.ready(awaitable, atMost)
    def awaitResult(implicit atMost: Duration): T = Await.result(awaitable, atMost)
  }
}
