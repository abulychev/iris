package com.github.abulychev.iris.dht.actor.remote

import akka.actor.{ActorRef, Props, Actor}
import java.net.InetSocketAddress
import com.github.abulychev.iris.dht.serialize.MessageSerializer
import com.github.abulychev.iris.dht.actor.Storage
import com.github.abulychev.iris.util.rpc.tcp.Server
import com.github.abulychev.iris.util.rpc.Rpc
import akka.util.ByteString

/**
 * User: abulychev
 * Date: 10/7/14
 */
private[dht] class StorageHandler(storage: ActorRef, endpoint: InetSocketAddress) extends Actor {
  val server = context.actorOf(Props(classOf[Server], endpoint, self), "server")

  def receive = {
    case Rpc.Request(bytes) =>
      val req = sender()

      MessageSerializer.fromBinary(bytes.toArray) match {
        case get @ Storage.Get(_, _) =>
          context.actorOf(Props(new Actor {
            override def preStart() {
              storage ! get
            }

            def receive = {
              case resp @ Storage.Response(_) =>
                val bytes = MessageSerializer.toBinary(resp)
                req ! Rpc.Response(bytes)
                context.stop(self)
            }
          }))

        case put @ Storage.PutBatch(_) =>
          storage ! put
          sender ! Rpc.Response(ByteString())

      }
  }

}

object StorageHandler {
  def props(storage: ActorRef, endpoint: InetSocketAddress): Props = Props(classOf[StorageHandler], storage, endpoint)
}
