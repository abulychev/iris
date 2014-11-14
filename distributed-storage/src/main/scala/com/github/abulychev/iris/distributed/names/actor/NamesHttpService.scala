package com.github.abulychev.iris.distributed.names.actor

import akka.actor.{ActorRef, Props, ActorLogging, Actor}
import spray.http.HttpEntity
import spray.http.MediaTypes._
import spray.http.HttpResponse
import java.net.InetSocketAddress
import spray.routing.HttpServiceActor
import com.github.abulychev.iris.distributed.names.serialize.{VersionsListSerializer, UpdateListSerializer}
import com.github.abulychev.iris.distributed.names.UpdateList

/**
 * User: abulychev
 * Date: 10/21/14
 */
class NamesHttpService(interface: String,
                       port: Int,
                       storage: ActorRef) extends HttpServiceActor with ActorLogging {

  val route = {
    path("get") { ctx =>
      val versions = VersionsListSerializer.fromBinary(ctx.request.entity.data.toByteArray)
      context.actorOf(Props(new Actor {
        storage ! versions

        def receive = {
          case updates @ UpdateList(_) =>
            val bytes = UpdateListSerializer.toBinary(updates)
            ctx.complete(HttpResponse(entity = HttpEntity(`application/octet-stream`, bytes)))
            context.stop(self)
        }
      }))
    }
  }



  def receive = runRoute(route)
}

object NamesHttpService {
  def props(interface: String, port: Int, storage: ActorRef): Props =
    Props(classOf[NamesHttpService], interface, port, storage)

  def props(endpoint: InetSocketAddress, storage: ActorRef): Props =
    props(endpoint.getHostName, endpoint.getPort, storage)
}
