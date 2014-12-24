package com.github.abulychev.iris.distributed.common.http

import akka.actor.{ActorLogging, Props, Actor, ActorRef}
import com.github.abulychev.iris.distributed.common.actor.DistributedStorage
import DistributedStorage.{Message, Response, Get}
import scala.util.Failure
import com.github.abulychev.iris.serialize.{OptionSerializer, Serializer}
import com.github.abulychev.iris.util.rpc.Rpc
import akka.util.ByteString

/**
 * User: abulychev
 * Date: 10/23/14
 */
class StorageHttpService[K,V] private (storage: ActorRef,
                                       keySerializer: Serializer[K],
                                       valueSerializer: Serializer[V],
                                       intoInternal: PartialFunction[Any, Any],
                                       fromInternal: PartialFunction[Message, Any]) extends Actor with ActorLogging {

  val responseSerializer = OptionSerializer(valueSerializer)

  def receive = {
    case Rpc.Request(bytes) =>
      val req = sender()

      val key = keySerializer.fromBinary(bytes.toArray)

      context.actorOf(Props(new Actor {
        storage ! fromInternal(Get(key))

        def receive = intoInternal andThen {
          case Failure(_) => Response(None)
          case x => x
        } andThen {
          case Response(result: Option[V]) =>
            req ! Rpc.Response(ByteString(responseSerializer.toBinary(result)))
            context.stop(self)
        }
      }))
  }
}

object StorageHttpService {
  def props[K,V](storage: ActorRef,
                 keySerializer: Serializer[K],
                 valueSerializer: Serializer[V],
                 intoInternal: PartialFunction[Any, Any],
                 fromInternal: PartialFunction[Message, Any]) =
    Props(classOf[StorageHttpService[K,V]],
      storage,
      keySerializer,
      valueSerializer,
      intoInternal,
      fromInternal
    )
}
