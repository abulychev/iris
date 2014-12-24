package com.github.abulychev.iris.util.rpc.tcp

import akka.actor._
import akka.io.{IO, Tcp}
import akka.util.ByteString
import com.github.abulychev.iris.serialize.IntSerializer
import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.collection.mutable
import com.github.abulychev.iris.util.rpc.RpcMessageSerializer
import java.net.InetSocketAddress
import com.github.abulychev.iris.util.rpc.RpcMessage

/**
 * User: abulychev
 * Date: 12/16/14
 */

abstract class Connection(writeTimeout: FiniteDuration = 10.seconds) extends FSM[Connection.State, Connection.Data] {
  import Connection._

  private val bufferOut = mutable.Queue.empty[BytesWithTS]
  // May be queue?
  private var bufferIn = ByteString()

  private case class BytesWithTS(bytes: ByteString, ts: Long)
  case object Ack extends Tcp.Event
  case object Check

  def handler : ActorRef

  setTimer("check", Check, writeTimeout, repeat = true)

  when(Connecting) {
    case Event(Tcp.CommandFailed(_: Tcp.Connect), _) =>
      stop()

    case Event(Tcp.Connected(remote, local), _) =>
      val socket = sender()
      socket ! Tcp.Register(self)
      
      if (hasOutMessages) {
        flushFirstMessage(socket)
        goto(WaitingAck) using ConnectedTo(socket)
      } else goto(Idle) using ConnectedTo(socket)

    case Event(msg: RpcMessage, _) =>
      val bytes = rpcMessageToByteString(msg)
      enqueueOutMessage(bytes)
      stay()
  }

  when(Idle) {
    case Event(msg: RpcMessage, ConnectedTo(socket)) =>
      val bytes = rpcMessageToByteString(msg)
      enqueueOutMessage(bytes)
      flushFirstMessage(socket)

      goto(WaitingAck)
  }

  when(WaitingAck) {
    case Event(Ack, ConnectedTo(socket)) =>
      acknowledgeFirst()

      if (hasOutMessages) {
        flushFirstMessage(socket)
        goto(WaitingAck)
      } else goto(Idle)

    case Event(msg: RpcMessage, ConnectedTo(socket)) =>
      val bytes = rpcMessageToByteString(msg)
      enqueueOutMessage(bytes)
      stay()
  }

  whenUnhandled {
    case Event(Tcp.Received(bytes), _) =>
      enqueueInMessage(bytes)
      bufferIn = processIn(bufferIn)
      stay()

    case Event(Tcp.PeerClosed, _) =>
      stop()

    case Event(_: Tcp.ErrorClosed, _) =>
      stop()

    case Event(Check, _) =>
      if (hasReachedTimeout) stop() else stay()
  }

  onTermination {
    case _: StopEvent => cancelTimer("check")
  }

  def hasOutMessages: Boolean = bufferOut.nonEmpty
  
  def flushFirstMessage(socket: ActorRef) {
    socket ! Tcp.Write(bufferOut.front.bytes, Ack)
  }
  
  def acknowledgeFirst() {
    bufferOut.dequeue()
  }

  def enqueueOutMessage(bytes: ByteString) {
    bufferOut.enqueue(BytesWithTS(bytes, System.currentTimeMillis))
  }

  def rpcMessageToByteString(msg: RpcMessage): ByteString = {
    val payload = ByteString(RpcMessageSerializer.toBinary(msg))
    val len = ByteString(IntSerializer.toBinary(payload.length))
    len ++ payload
  }

  def hasReachedTimeout: Boolean =
    hasOutMessages && bufferOut.front.ts + writeTimeout.toMillis > System.currentTimeMillis

  private def enqueueInMessage(bytes: ByteString) {
    bufferIn ++= bytes
  }

  @tailrec
  private def processIn(bytes: ByteString): ByteString = {
    if (bytes.length < 4) bytes
    else {
      val len = IntSerializer.fromBinary(bytes.take(4).toArray)
      if (bytes.length < 4 + len) bytes
      else {
        val payload = bytes.drop(4).take(len)
        val msg = RpcMessageSerializer.fromBinary(payload.toArray)
        handler ! msg
        processIn(bytes.drop(4).drop(len))
      }
    }
  }
}

object Connection {
  sealed trait State
  case object Connecting extends State
  case object Idle extends State
  case object WaitingAck extends State

  sealed trait Data
  case object NotConnected extends Data
  case class ConnectedTo(socket: ActorRef) extends Data

//  case object WriteTimeout
}

class ClientConnection(endpoint: InetSocketAddress, val handler: ActorRef) extends Connection {
  import Connection._

  import context.system
  IO(Tcp) ! Tcp.Connect(endpoint)

  startWith(Connecting, NotConnected)
  initialize()
}

class ServerConnection(socket: ActorRef, delegate: ActorRef) extends Connection {
  import Connection._

  val handler = context.actorOf(Props(classOf[ServerHandler], delegate, self, 15.seconds), "handler")

  startWith(Idle, ConnectedTo(socket))
}