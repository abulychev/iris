package com.github.abulychev.iris.distributed.common.http

import akka.actor._
import java.net.InetSocketAddress
import spray.can.Http
import akka.io.IO
import spray.http.{HttpResponse, HttpEntity, Uri, HttpRequest}
import spray.http.HttpMethods._
import spray.http.MediaTypes._
import scala.concurrent.duration._
import com.github.abulychev.iris.distributed.common.actor.DistributedStorage
import DistributedStorage.Response
import scala.util.Failure
import com.github.abulychev.iris.serialize.Serializer

/**
 * User: abulychev
 * Date: 10/23/14
 */
class StorageHttpClient[K,V](endpoint: InetSocketAddress,
                             prefix: String,
                             key: K,
                             keySerializer: Serializer[K],
                             responseSerializer: Serializer[Option[V]],
                             req: ActorRef) extends Actor with ActorLogging {

  private val url = s"http://${endpoint.getHostName}:${endpoint.getPort}/$prefix/get"

  override def preStart() {
    import context.system
    val bytes = keySerializer.toBinary(key)
    IO(Http) ! HttpRequest(GET, Uri(url), entity = HttpEntity(`application/octet-stream`, bytes))
    context.setReceiveTimeout(10 seconds)
  }

  def receive: Receive = {
    case response: HttpResponse =>
      val value = responseSerializer.fromBinary(response.entity.data.toByteArray)
      req ! Response(value)
      context.stop(self)

    case ReceiveTimeout =>
      req ! Failure(new Error("Timeout"))
      context.stop(self)
  }
}

object StorageHttpClient {
  def props[K,V](endpoint: InetSocketAddress,
                 prefix: String,
                 key: K,
                 keySerializer: Serializer[K],
                 valueSerializer: Serializer[Option[V]],
                 req: ActorRef) =
    Props(classOf[StorageHttpClient[K,V]],
      endpoint,
      prefix,
      key,
      keySerializer,
      valueSerializer,
      req
    )
}
