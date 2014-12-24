package com.github.abulychev.iris.util.rpc.udp

import akka.actor.{ReceiveTimeout, ActorRef, ActorLogging, Actor}
import com.github.abulychev.iris.util.rpc.{RpcMessageSerializer, RpcMessage, Rpc}
import scala.concurrent.duration._
import java.net.InetSocketAddress
import akka.io.Udp
import akka.util.ByteString

/**
 * User: abulychev
 * Date: 12/24/14
 */
abstract class UdpTask(socket: ActorRef, endpoint: InetSocketAddress, reqid: Long) extends Actor with ActorLogging {

  def asking(req: Rpc.Request, handler: ActorRef, first: Boolean = true): Receive = {
    val message = RpcMessage(reqid, req.bytes, first, last = false)
    val data = ByteString(RpcMessageSerializer.toBinary(message))

    socket ! Udp.Send(data, endpoint)

    context.setReceiveTimeout(req.timeout)

    PartialFunction {
      case RpcMessage(_, bytes, _, true) =>
        handler ! Rpc.Response(bytes)
        context.stop(self)

      case msg @ RpcMessage(_, bytes, _, false) =>
        context.become(responding(msg, handler))

      case ReceiveTimeout =>
        handler ! Rpc.Timeout
        context.stop(self)
    }
  }

  def responding(msg: RpcMessage, handler: ActorRef): Receive = {
    require(!msg.last)

    handler ! Rpc.Request(msg.bytes)
    context.setReceiveTimeout(1.minute)

    PartialFunction {
      case Rpc.Response(bytes) =>
        val message = RpcMessage(reqid, bytes, first = false, last = true)
        val data = ByteString(RpcMessageSerializer.toBinary(message))
        socket ! Udp.Send(data, endpoint)

        context.stop(self)

      case r: Rpc.Request =>
        context.become(asking(r, sender(), first = false))

      case ReceiveTimeout =>
        context.stop(self)
    }
  }
}

class ClientUdpTask(handler: ActorRef,
                 socket: ActorRef,
                 endpoint: InetSocketAddress,
                 request: Rpc.Request,
                 reqid: Long) extends UdpTask(socket, endpoint, reqid) {

  def receive = asking(request, handler)
}

class ServerUdpTask(handler: ActorRef,
                 socket: ActorRef,
                 endpoint: InetSocketAddress,
                 request: RpcMessage,
                 reqid: Long) extends UdpTask(socket, endpoint, reqid) {

  def receive = responding(request, handler)
}
