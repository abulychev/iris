package com.github.abulychev.iris.distributed.routing

import akka.actor.{Props, ActorRef, ActorLogging, Actor}
import com.github.abulychev.iris.distributed.names.actor.VersionsRoutingActor
import java.net.InetSocketAddress
import scala.collection.mutable
import com.github.abulychev.iris.distributed.info.actor.InfoRoutingActor
import scala.util.Failure
import com.github.abulychev.iris.distributed.chunk.actor.ChunkRoutingActor

/**
 * User: abulychev
 * Date: 10/30/14
 */
class RoutingService(versions: ActorRef,
                     info: ActorRef,
                     chunk: ActorRef) extends Actor with ActorLogging {

  val available = mutable.Set.empty[InetSocketAddress]

  def receive = {
    /* Versions */
    case update @ VersionsRoutingActor.UpdateVersion(_, _) =>
      versions forward update

    case req @ VersionsRoutingActor.NodeRequest(_) =>
      forwardVersions(req)

    /* Info */
    case add @ InfoRoutingActor.AddRoute(_) =>
      info forward add

    case req @ InfoRoutingActor.GetRoutes(_) =>
      forwardInfo(req)

    /* Chunk */
    case add @ ChunkRoutingActor.AddRoute(_) =>
      chunk forward add

    case req @ ChunkRoutingActor.GetRoutes(_) =>
      forwardChunk(req)

    /* Reachability */
    case RoutingService.Reachable(endpoint) =>
      available += endpoint

    case RoutingService.Unreachable(endpoint) =>
      available -= endpoint

    case msg => log.error(s"Unhandled message: {$msg}")
  }

  def forwardVersions(req: VersionsRoutingActor.NodeRequest) =
    forward[VersionsRoutingActor.NodeResponse](sender(), versions, req)(_.endpoints, VersionsRoutingActor.NodeResponse)

  def forwardInfo(req: InfoRoutingActor.GetRoutes) =
    forward[InfoRoutingActor.RoutesResponse](sender(), info, req)(_.routes, InfoRoutingActor.RoutesResponse)

  def forwardChunk(req: ChunkRoutingActor.GetRoutes) =
    forward[ChunkRoutingActor.RoutesResponse](sender(), chunk, req)(_.routes, ChunkRoutingActor.RoutesResponse)

  def forward[K](from: ActorRef, to: ActorRef, req: Any)(unpack: K => List[InetSocketAddress],
                                                         pack: List[InetSocketAddress] => K) {

    val alive = available.toSet

    context.actorOf(Props(new Actor {
      to ! req
      def receive = {
        case failure @ Failure(_) =>
          from ! failure

        case resp: K =>
          val endpoints = unpack(resp).filter(alive)
          // TODO: sort by timings
          from ! pack(endpoints)
      }
    }))
  }
}

object RoutingService {
  case class Reachable(endpoint: InetSocketAddress)
  case class Unreachable(endpoint: InetSocketAddress)
}
