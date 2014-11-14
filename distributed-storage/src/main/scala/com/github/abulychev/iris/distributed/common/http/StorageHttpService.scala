package com.github.abulychev.iris.distributed.common.http

import spray.routing.HttpServiceActor
import akka.actor.{Props, Actor, ActorRef}
import spray.http.HttpEntity
import spray.http.MediaTypes._
import spray.http.HttpResponse
import com.github.abulychev.iris.distributed.common.actor.DistributedStorage
import DistributedStorage.{Message, Response, Get}
import scala.util.Failure
import com.github.abulychev.iris.serialize.{OptionSerializer, Serializer}

/**
 * User: abulychev
 * Date: 10/23/14
 */
class StorageHttpService[K,V] private (storage: ActorRef,
                                       keySerializer: Serializer[K],
                                       valueSerializer: Serializer[V],
                                       intoInternal: PartialFunction[Any, Any],
                                       fromInternal: PartialFunction[Message, Any]) extends HttpServiceActor {

  val responseSerializer = OptionSerializer(valueSerializer)

  val route = {
    path("get") {
      get { ctx =>
        context.actorOf(Props(new Actor {
          val bytes = ctx.request.entity.data.toByteArray
          val key = keySerializer.fromBinary(bytes)

          storage ! fromInternal(Get(key))

          def receive = intoInternal andThen {
            case Failure(_) => Response(None)
            case x => x
          } andThen {
            case Response(result: Option[V]) =>
              val entity = HttpEntity(`application/octet-stream`, responseSerializer.toBinary(result))
              ctx.complete(HttpResponse(entity = entity))
              context.stop(self)
          }
        }))
      }
    }
  }

  def receive = runRoute(route)
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
