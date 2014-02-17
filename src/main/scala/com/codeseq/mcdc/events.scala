package com.codeseq.mcdc

import akka.actor.ActorRef
import scala.collection.mutable

object EventPublisher {
  case class Register(ref: ActorRef)
  case class Unregister(ref: ActorRef)
}

class EventPublisher {
  private val listeners = mutable.HashSet.empty[ActorRef]

  def +(ref: ActorRef): Unit = listeners += ref
  def -(ref: ActorRef): Unit = listeners -= ref
  def publish(msg: Any): Unit = listeners foreach { _ ! msg }
}
