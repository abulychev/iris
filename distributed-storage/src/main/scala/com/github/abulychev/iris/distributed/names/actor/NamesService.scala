package com.github.abulychev.iris.distributed.names.actor

import akka.actor.{ActorRef, Props, ActorLogging, Actor}
import com.github.abulychev.iris.distributed.names.serialize.{VersionsListSerializer, UpdateListSerializer}
import com.github.abulychev.iris.distributed.names.UpdateList
import com.github.abulychev.iris.util.rpc.Rpc
import akka.util.ByteString

/**
 * User: abulychev
 * Date: 10/21/14
 */
class NamesService(storage: ActorRef) extends Actor with ActorLogging {

  def receive = {
    case Rpc.Request(bytes) =>
      val req = sender()
      val versions = VersionsListSerializer.fromBinary(bytes.toArray)

      context.actorOf(Props(new Actor {
        storage ! versions

        def receive = {
          case updates @ UpdateList(_) =>
            val bytes = ByteString(UpdateListSerializer.toBinary(updates))
            req ! Rpc.Response(bytes)
            context.stop(self)
        }
      }))
  }
}

object NamesService {
  def props(storage: ActorRef): Props = Props(classOf[NamesService], storage)
}
