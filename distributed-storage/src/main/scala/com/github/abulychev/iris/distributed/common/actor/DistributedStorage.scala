package com.github.abulychev.iris.distributed.common.actor

import akka.actor.{Props, ActorRef, Actor, ActorLogging}
import scala.util.Failure
import java.net.InetSocketAddress
import com.github.abulychev.iris.distributed.common.http.{StorageHttpService, StorageHttpClient}
import com.github.abulychev.iris.serialize.{OptionSerializer, Serializer}
import com.github.abulychev.iris.{Service, RegisterService}

/**
 * User: abulychev
 * Date: 10/23/14
 */
abstract class DistributedStorage[K,V](storage: ActorRef,
                                       routingService: ActorRef,
                                       service: Service,
                                       keySerializer: Serializer[K],
                                       valueSerializer: Serializer[V],
                                       registry: ActorRef) extends Actor with ActorLogging {
  
  import DistributedStorage._
  
  def translateIntoMessage: PartialFunction[Any, Message]
  def translateFromMessage: PartialFunction[Message, Any]

  private def intoInternal: PartialFunction[Any, Any] = translateIntoMessage orElse PartialFunction { x => x }
  private def fromInternal = translateFromMessage

  private val responseSerializer = OptionSerializer(valueSerializer)

  override def preStart() {
    val http = context.actorOf(StorageHttpService.props(
      storage,
      keySerializer,
      valueSerializer,
      intoInternal,
      fromInternal
    ))

    registry ! RegisterService(service, http)
    storage ! fromInternal(GetAllKeys)
  }

  def receive = intoInternal andThen PartialFunction[Any, Unit]{
    case get @ Get(key: K) =>
      val req = sender()
      context.actorOf(Props(new Actor {
        storage ! translateFromMessage(get)

        def receive = intoInternal andThen {
          case resp @ Response(Some(_)) =>
            req ! fromInternal(resp)
            context.stop(self)

          case Failure(_) | Response(None) =>
            routingService ! fromInternal(GetRoutes(key))
            context.become(waitingRoutes)
        }

        def waitingRoutes: Receive = intoInternal andThen {
          case failure @ Failure(_) =>
            req ! failure
            context.stop(self)

          case RoutesResponse(routes) =>
            context.become(asking(routes))
        }

        def asking(routes: List[InetSocketAddress]): Receive = {
          if (routes.length == 0) {
            req ! Failure(new Error("Couldn't receive data"))
            context.stop(self)
          } else {
            context.actorOf(StorageHttpClient.props(
              routes.head,
              service,
              key,
              keySerializer,
              responseSerializer,
              self
            ))
          }

          PartialFunction[Any,Unit]{
            case resp @ Response(Some(value)) =>
              req ! fromInternal(resp)
              storage ! fromInternal(Put(key, value))
              routingService ! fromInternal(AddRoute(key))
              context.stop(self)

            case Failure(_) | Response(None) =>
              context.become(asking(routes.tail))
          }
        }
      }))


    case put @ Put(key: K, value: V) =>
      val req = sender()
      context.actorOf(Props(new Actor {
        storage ! translateFromMessage(put)
        
        def receive = intoInternal andThen {
          case failure @ Failure(_) =>
            req ! failure
            context.stop(self)

          case Acknowledged =>
            req ! fromInternal(Acknowledged)
            routingService ! fromInternal(AddRoute(key))
            context.stop(self)
        }
      }))

    case AllKeys(keys) =>
      keys foreach { key => routingService ! fromInternal(AddRoute(key)) }
  }
}

object DistributedStorage {
  sealed trait Message

  case class Get[K](key: K) extends Message
  case class Put[K,V](key: K, value: V) extends Message
  case class Response[V](result: Option[V]) extends Message
  case object Acknowledged extends Message
  case object GetAllKeys extends Message
  case class AllKeys[K](keys: Set[K]) extends Message

  case class AddRoute[K](key: K) extends Message
  case class GetRoutes[K](key: K) extends Message
  case class RoutesResponse(routes: List[InetSocketAddress]) extends Message
  //case object RouteAcknowledged extends Message
}
