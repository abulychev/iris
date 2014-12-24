package com.github.abulychev.iris.dht.actor.remote

import akka.actor.{Props, Actor}
import java.net.InetSocketAddress
import com.github.abulychev.iris.dht.serialize.MessageSerializer
import scala.util.Failure
import com.github.abulychev.iris.dht.actor.Storage
import com.github.abulychev.iris.settings.IrisSettings
import com.github.abulychev.iris.util.rpc.tcp.ClientManager
import com.github.abulychev.iris.util.rpc.{Rpc, ClientBuilder}
import com.github.abulychev.iris.util.rpc.ClientBuilder._
import scala.concurrent.duration._

/**
 * User: abulychev
 * Date: 10/7/14
 */
private[dht] class StorageClient private(endpoint: InetSocketAddress) extends Actor with IrisSettings  {
  val clients = context.actorOf(Props(classOf[ClientManager]), "clients")
  implicit val cb = ClientBuilder(clients)

  def receive = {
    case get: Storage.Get =>
      val req = sender()
      context.actorOf(Props(new Actor {
        client(endpoint) ! Rpc.Request(MessageSerializer.toBinary(get), 5.seconds)

        def receive = {
          case Rpc.Response(bytes) =>
            val resp = MessageSerializer.fromBinary(bytes.toArray)
            req ! resp
            context.stop(self)

          case Rpc.Timeout =>
            req ! Failure(new Exception)
            context.stop(self)
        }
      }))

    case put: Storage.Put =>
      self forward Storage.PutBatch(List(put))

    case put: Storage.PutBatch =>
      context.actorOf(Props(new Actor {
        client(endpoint) ! Rpc.Request(MessageSerializer.toBinary(put), 5.seconds)

        def receive = {
          case _: Rpc.Response =>
            context.stop(self)

          case Rpc.Timeout =>
            context.stop(self)
        }
      }))
  }
}

object StorageClient {
  def props(endpoint: InetSocketAddress) = Props(classOf[StorageClient], endpoint)
}
