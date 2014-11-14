package com.github.abulychev.iris.dht.actor.http

import akka.actor.{Status, Props, Actor}
import java.net.InetSocketAddress
import akka.io.IO
import spray.can.Http
import spray.http._
import spray.http.HttpMethods._
import spray.http.MediaTypes._
import spray.http.HttpRequest
import com.github.abulychev.iris.dht.serialize.{PutSerializer, ResponseSerializer, GetSerializer}
import scala.util.Failure
import com.github.abulychev.iris.dht.actor.Storage
import com.github.abulychev.iris.serialize.ListSerializer
import com.github.abulychev.iris.settings.{HostConnectorSetupBuilder, IrisSettings}

/**
 * User: abulychev
 * Date: 10/7/14
 */
private[dht] class HttpStorageClient private(endpoint: InetSocketAddress) extends Actor with IrisSettings  {
  private val setup = HostConnectorSetupBuilder(
    connectingTimeout = settings.Dht.ConnectingTimeout,
    requestTimeout = settings.Dht.RequestTimeout
  ).build(
    host = endpoint.getHostName,
    port = endpoint.getPort
  )(context.system)

  def receive = {
    case get @ Storage.Get(_, _) =>
      val req = sender()
      context.actorOf(Props(new Actor {
        override def preStart() {
          import context.system
          val url = s"http://${endpoint.getHostName}:${endpoint.getPort}/get"
          val bytes = GetSerializer.toBinary(get)
          IO(Http) ! (HttpRequest(GET, Uri(url), entity = HttpEntity(`application/octet-stream`, bytes)) -> setup)
        }

        def receive = {
          case response: HttpResponse =>
            val resp = ResponseSerializer.fromBinary(response.entity.data.toByteArray)
            req ! resp
            context.stop(self)

          case Timedout(_) =>
            req ! Failure(new Exception)
            context.stop(self)
        }
      }))

    case put @ Storage.Put(_, _, _) =>
      self forward Storage.PutBatch(List(put))

    case Storage.PutBatch(batch) =>
      context.actorOf(Props(new Actor {
        override def preStart() {
          import context.system
          val url = s"http://${endpoint.getHostName}:${endpoint.getPort}/put"
          val bytes = ListSerializer(PutSerializer).toBinary(batch)
          IO(Http) ! (HttpRequest(GET, Uri(url), entity = HttpEntity(`application/octet-stream`, bytes)) -> setup)
        }

        def receive = {
          case response: HttpResponse =>
            context.stop(self)

          case Status.Failure(e) =>
            context.stop(self)
        }
      }))
  }
}

object HttpStorageClient {
  def props(endpoint: InetSocketAddress) = Props(classOf[HttpStorageClient], endpoint)
}
