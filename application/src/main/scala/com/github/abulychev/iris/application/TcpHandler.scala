package com.github.abulychev.iris.application

import java.net.InetSocketAddress
import akka.actor.{Actor, ActorLogging, Props, ActorRef}
import com.github.abulychev.iris.util.rpc.Rpc
import com.github.abulychev.iris.util.rpc.tcp.Server
import com.github.abulychev.iris.Service


/**
 * User: abulychev
 * Date: 10/24/14
 */
class TcpHandler private(localAddress: InetSocketAddress,
                         services: Map[Byte, ActorRef]) extends Actor with ActorLogging {

  log.info(s"Bounding tcp socket at $localAddress")

  context.actorOf(Props(classOf[Server], localAddress, self), "socket")

  def receive = {
    case Rpc.Push(bytes) if bytes.length > 0 =>
      val code = bytes.head
      val service = services.get(code)

      if (service.nonEmpty) service.get forward Rpc.Push(bytes.tail)
      else log.error(s"Service with code ($code) not found")

    case Rpc.Request(bytes) if bytes.length > 0 =>
      val code = bytes.head
      val service = services.get(code)

      if (service.nonEmpty) service.get forward Rpc.Request(bytes.tail)
      else log.error(s"Service with code ($code) not found")

    case msg => log.error(s"Unhandled message: $msg")
  }
}

object TcpHandler {
  def props(localAddress: InetSocketAddress, services: Map[Service, ActorRef]) =
    Props(classOf[TcpHandler],
      localAddress,
      services map { case (service, actor) => service.code -> actor }
    )
}