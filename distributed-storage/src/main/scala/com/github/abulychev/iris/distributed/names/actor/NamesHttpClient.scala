package com.github.abulychev.iris.distributed.names.actor

import akka.actor.{Props, ActorLogging, Actor}
import java.net.InetSocketAddress
import scala.concurrent.duration._
import scala.util.Failure
import com.github.abulychev.iris.distributed.names.serialize.{VersionsListSerializer, UpdateListSerializer}
import com.github.abulychev.iris.distributed.names.VersionsList
import com.github.abulychev.iris.util.rpc.tcp.ClientManager
import com.github.abulychev.iris.util.rpc.{Rpc, ClientBuilder}
import com.github.abulychev.iris.util.rpc.ClientBuilder._
import akka.util.ByteString

/**
 * User: abulychev
 * Date: 10/21/14
 */
class NamesHttpClient private(endpoint: InetSocketAddress) extends Actor with ActorLogging {
  // Make it common
  val clients = context.actorOf(Props(classOf[ClientManager]), "clients")
  implicit val cb = ClientBuilder(clients)

  def receive = {
    case versions @ VersionsList(_) =>
      val req = sender()

      context.actorOf(Props(new Actor {
        override def preStart() {
          val bytes = ByteString(VersionsListSerializer.toBinary(versions))
          client(endpoint) ! Rpc.Request(ByteString(10) ++ bytes, 10.seconds)
        }

        def receive: Receive = {
          case Rpc.Response(bytes) =>
            val updates = UpdateListSerializer.fromBinary(bytes.toArray)
            req ! updates
            context.stop(self)

          case Rpc.Timeout =>
            req ! Failure(new Exception)
            context.stop(self)
        }
      }))
  }
}

object NamesHttpClient {
  def props(endpoint: InetSocketAddress) = Props(classOf[NamesHttpClient], endpoint)
}
