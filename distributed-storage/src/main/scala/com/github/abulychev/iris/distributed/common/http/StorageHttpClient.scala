package com.github.abulychev.iris.distributed.common.http

import akka.actor._
import java.net.InetSocketAddress
import scala.concurrent.duration._
import com.github.abulychev.iris.distributed.common.actor.DistributedStorage
import DistributedStorage.Response
import scala.util.Failure
import com.github.abulychev.iris.serialize.Serializer
import com.github.abulychev.iris.util.rpc.tcp.ClientManager
import com.github.abulychev.iris.util.rpc.{Rpc, ClientBuilder}
import com.github.abulychev.iris.util.rpc.ClientBuilder._
import akka.util.ByteString
import com.github.abulychev.iris.Service

/**
 * User: abulychev
 * Date: 10/23/14
 */
class StorageHttpClient[K,V](endpoint: InetSocketAddress,
                             service: Service,
                             key: K,
                             keySerializer: Serializer[K],
                             responseSerializer: Serializer[Option[V]],
                             req: ActorRef) extends Actor with ActorLogging {

  // Make it common
  val clients = context.actorOf(Props(classOf[ClientManager]), "clients")
  implicit val cb = ClientBuilder(clients)

  override def preStart() {
    val bytes = ByteString(keySerializer.toBinary(key))
    client(endpoint) ! Rpc.Request(ByteString(service.code) ++ bytes, 10.seconds)
  }

  def receive: Receive = {
    case Rpc.Response(bytes) =>
      val value = responseSerializer.fromBinary(bytes.toArray)
      req ! Response(value)
      context.stop(self)

    case Rpc.Timeout =>
      req ! Failure(new Error("Timeout"))
      context.stop(self)
  }
}

object StorageHttpClient {
  def props[K,V](endpoint: InetSocketAddress,
                 service: Service,
                 key: K,
                 keySerializer: Serializer[K],
                 valueSerializer: Serializer[Option[V]],
                 req: ActorRef) =
    Props(classOf[StorageHttpClient[K,V]],
      endpoint,
      service,
      key,
      keySerializer,
      valueSerializer,
      req
    )
}
