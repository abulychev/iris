package com.github.abulychev.iris.dht.actor.http

import akka.actor.{ActorRef, Props, Actor}
import spray.can.Http
import akka.io.IO
import java.net.InetSocketAddress
import spray.http._
import spray.http.HttpMethods._
import spray.http.MediaTypes._
import spray.http.HttpRequest
import spray.http.HttpResponse
import com.github.abulychev.iris.dht.serialize.{PutSerializer, ResponseSerializer, GetSerializer}
import com.github.abulychev.iris.dht.actor.Storage
import com.github.abulychev.iris.serialize.ListSerializer

/**
 * User: abulychev
 * Date: 10/7/14
 */
private[dht] class HttpStorageHandler(storage: ActorRef, interface: String, port: Int) extends Actor {
  override def preStart() {
    import context.system
    IO(Http) ! Http.Bind(self, interface, port)
  }

  def receive = {
    case _: Http.Connected => sender ! Http.Register(self)

    case HttpRequest(GET, Uri.Path("/get"), _, entity, _) =>
      val get = GetSerializer.fromBinary(entity.data.toByteArray)
      val req = sender()

      context.actorOf(Props(new Actor {
        override def preStart() {
          storage ! get
        }

        def receive = {
          case resp @ Storage.Response(_) =>
            val bytes = ResponseSerializer.toBinary(resp)
            req ! HttpResponse(entity = HttpEntity(`application/octet-stream`, bytes))
            context.stop(self)
        }
      }))

    case HttpRequest(GET, Uri.Path("/put"), _, entity, _) =>
      val batch = Storage.PutBatch(
        ListSerializer(PutSerializer).fromBinary(entity.data.toByteArray)
      )
      storage ! batch

      sender ! HttpResponse(entity = HttpEntity(`application/octet-stream`, new Array[Byte](0)))
  }

}

object HttpStorageHandler {
  def props(storage: ActorRef, interface: String, port: Int): Props = Props(classOf[HttpStorageHandler], storage, interface, port)
  def props(storage: ActorRef, endpoint: InetSocketAddress): Props = props(storage, endpoint.getHostName, endpoint.getPort)
}
