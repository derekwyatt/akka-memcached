package com.codeseq.mcdc

import akka.actor.ExtensionId
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, FSM, Props}
import akka.io.IO.Extension
import akka.io.{IO, Tcp, PipelineFactory, PipelineContext, PipelinePorts}
import akka.util.ByteString
import java.net.InetSocketAddress
import scala.concurrent.duration._
import scala.collection.mutable

trait IOPRovider {
  def io[T <: Extension](key: ExtensionId[T])(implicit system: ActorSystem): ActorRef
}

trait IOPRoviderImpl extends IOPRovider {
  def io[T <: Extension](key: ExtensionId[T])(implicit system: ActorSystem): ActorRef = IO(key)
}

trait EventPipeline {
  private val PipelinePorts(_, evts, _) = PipelineFactory.buildFunctionTriple(new PipelineContext {},
        new MemcacheProtocol.MemcacheMessage >> new MemcacheProtocol.MemcacheFrame)

  def events(bs: ByteString): Iterable[Response] = evts(bs)._1
}

case class HostInfo(host: String, port: Int)

object MemcachedClientActor {

  sealed trait State
  case object ClientNotConnected extends State
  case object ClientConnected extends State

  sealed trait Data
  case class ConnectInfo(connectAttempts: Int, clients: Map[Int, ActorRef],
                         maxTag: Int, connection: Option[ActorRef]) extends Data

  private case class TryConnect()

  case class Send(req: Request)
  case class ConnectionUp(host: String, port: Int)
  case class ConnectionDown(host: String, port: Int)

  trait MemcachedClientActor extends Actor with ActorLogging
                                           with FSM[State, Data]
                                           with EventPipeline
                                           with Implicits { this: IOPRovider =>
    import Tcp._
    import context.system // needed by IO
    import context.dispatcher // this is our future/scheduler execution context

    val host: String
    val port: Int

    val publisher = new EventPublisher

    val address = new InetSocketAddress(host, port)

    def connect(): Unit = io(Tcp) ! Connect(address)

    override def preStart(): Unit = connect()

    startWith(ClientNotConnected, ConnectInfo(0, Map.empty, 0, None))

    when(ClientNotConnected) {
      case Event(CommandFailed(_: Connect), info @ ConnectInfo(count, _, _, _)) =>
        log.warning(s"Failed to connect to $host:$port - count:${count + 1}")
        context.system.scheduler.scheduleOnce(10.seconds, self, TryConnect())
        stay using info.copy(connectAttempts = count + 1, connection = None)

      case Event(TryConnect(), ConnectInfo(count, _, _, _)) =>
        connect()
        stay

      case Event(c @ Connected(remote, local), info @ ConnectInfo(count, _, _, _)) =>
        log.info(s"Connected to $host:$port after $count attempts")
        val connection = sender
        connection ! Register(self)
        goto(ClientConnected) using info.copy(connectAttempts = 0, connection = Some(connection))
    }

    onTransition {
      case ClientNotConnected -> ClientConnected =>
        publisher.publish(ConnectionUp(host, port))
      case ClientConnected -> ClientNotConnected =>
        publisher.publish(ConnectionDown(host, port))
    }

    when(ClientConnected) {
      case Event(Send(request), info @ ConnectInfo(_, known, tag, Some(connection))) =>
        val recipient = sender
        connection ! Write(request.copy(header = request.header.copy(opaque = tag)).toByteString)
        val nextTag = if (tag == Int.MaxValue) 0 else tag + 1
        stay using info.copy(clients = known + (tag -> recipient), maxTag = nextTag)
      
      case Event(Received(data), info @ ConnectInfo(_, known, _, _)) =>
        val newKnown = events(data).foldLeft(known) { (map, rsp) =>
          val tag = rsp.header.opaque
          val recipient = known.get(tag)
          if (recipient.isDefined)
            recipient.get ! rsp
          else
            log.warning(s"Received response for tag $tag that has no known recipient. Discarding.")
          (map - tag)
        }
        stay using info.copy(clients = newKnown)

      case Event(_: ConnectionClosed, info: ConnectInfo) =>
        log.warning(s"Connection closed from $host:$port.")
        self ! TryConnect()
        goto(ClientNotConnected) using info.copy(connectAttempts = 0, connection = None)

      case Event(EventPublisher.Register(ref), _) =>
        publisher + ref
        ref ! ConnectionUp(host, port)
        stay
    }

    whenUnhandled {
      case Event(EventPublisher.Unregister(ref), _) =>
        publisher - ref
        stay
      case Event(EventPublisher.Register(ref), _) =>
        publisher + ref
        stay
    }

    initialize()
  }

  def props(host: String, port: Int): Props = {
    val h = host
    val p = port
    Props(new MemcachedClientActor with IOPRoviderImpl {
      lazy val host = h
      lazy val port = p
    })
  }
}
