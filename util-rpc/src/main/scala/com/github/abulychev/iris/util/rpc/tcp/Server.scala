package com.github.abulychev.iris.util.rpc.tcp

import akka.actor.{Props, ActorRef, ActorLogging, Actor}
import java.net.InetSocketAddress
import akka.io.{IO, Tcp}

/**
 * User: abulychev
 * Date: 12/15/14
 */
class Server(address: InetSocketAddress, handler: ActorRef) extends Actor with ActorLogging {
  import context.system
  IO(Tcp) ! Tcp.Bind(self, address)

  def receive = {
    case b @ Tcp.Bound(localAddress) =>

    case Tcp.CommandFailed(_: Tcp.Bind) => context stop self

    case c @ Tcp.Connected(remote, local) =>
      val socket = sender()
      val connection = context.actorOf(Props(classOf[ServerConnection], socket, handler))
      socket ! Tcp.Register(connection)
  }
}
