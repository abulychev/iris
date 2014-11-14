package com.github.abulychev.iris.distributed.names.actor

import akka.actor.{ReceiveTimeout, Props, ActorLogging, Actor}
import java.net.InetSocketAddress
import akka.io.IO
import spray.can.Http
import spray.http.{HttpResponse, HttpEntity, Uri, HttpRequest}
import spray.http.HttpMethods._
import spray.http.MediaTypes._
import scala.concurrent.duration._
import scala.util.Failure
import com.github.abulychev.iris.distributed.names.serialize.{VersionsListSerializer, UpdateListSerializer}
import com.github.abulychev.iris.distributed.names.VersionsList

/**
 * User: abulychev
 * Date: 10/21/14
 */
class NamesHttpClient private(endpoint: InetSocketAddress) extends Actor with ActorLogging {
  private val url = s"http://${endpoint.getHostName}:${endpoint.getPort}/names/get"

  def receive = {
    case versions @ VersionsList(_) =>
      val req = sender()

      context.actorOf(Props(new Actor {
        override def preStart() {
          import context.system
          val bytes = VersionsListSerializer.toBinary(versions)
          IO(Http) ! HttpRequest(GET, Uri(url), entity = HttpEntity(`application/octet-stream`, bytes))
          context.setReceiveTimeout(10 seconds)
        }

        def receive: Receive = {
          case response: HttpResponse =>
            val updates = UpdateListSerializer.fromBinary(response.entity.data.toByteArray)
            req ! updates
            context.stop(self)

          case ReceiveTimeout =>
            req ! Failure(new Exception)
            context.stop(self)
        }
      }))
  }
}

object NamesHttpClient {
  def props(endpoint: InetSocketAddress) = Props(classOf[NamesHttpClient], endpoint)
}
