package com.github.abulychev.iris.util.rpc.tcp

import akka.actor.{ReceiveTimeout, ActorRef, ActorLogging, Actor}
import com.github.abulychev.iris.util.rpc.{RpcMessage, Rpc}
import scala.concurrent.duration._

/**
 * User: abulychev
 * Date: 12/24/14
 */
abstract class TcpTask(connection: ActorRef, reqid: Long) extends Actor with ActorLogging {

  def asking(req: Rpc.Request, handler: ActorRef, first: Boolean = true): Receive = {
    connection ! RpcMessage(reqid, req.bytes, first, last = false)
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
        connection ! RpcMessage(reqid, bytes, first = false, last = true)
        context.stop(self)

      case r: Rpc.Request =>
        context.become(asking(r, sender(), first = false))

      case ReceiveTimeout =>
        context.stop(self)
    }
  }
}

class ClientTcpTask(handler: ActorRef,
                    connection: ActorRef,
                    request: Rpc.Request,
                    reqid: Long) extends TcpTask(connection, reqid) {

  def receive = asking(request, handler)
}

class ServerTcpTask(handler: ActorRef,
                 connection: ActorRef,
                 request: RpcMessage,
                 reqid: Long) extends TcpTask(connection, reqid) {

  def receive = responding(request, handler)
}
