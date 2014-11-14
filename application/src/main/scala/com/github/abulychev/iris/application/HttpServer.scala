package com.github.abulychev.iris.application

import spray.routing.HttpServiceActor
import java.net.InetAddress
import akka.actor.{Props, ActorRef}
import akka.io.IO
import spray.can.Http


/**
 * User: abulychev
 * Date: 10/24/14
 */
class HttpServer private(host: InetAddress,
                         port: Int,
                         services: Map[String, ActorRef]) extends HttpServiceActor {

  IO(Http)(context.system) ! Http.Bind(self, host.getHostName, port)

  val route = {
    pathPrefix(Segment) { service =>
      ctx => services.get(service) foreach { _ ! ctx }
    }
  }

  def receive = runRoute(route)
}

object HttpServer {
  def props(host: InetAddress, port: Int, services: Map[String, ActorRef]) =
    Props(classOf[HttpServer], host, port, services)
}