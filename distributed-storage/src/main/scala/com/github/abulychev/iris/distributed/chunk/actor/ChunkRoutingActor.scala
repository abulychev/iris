package com.github.abulychev.iris.distributed.chunk.actor

import akka.actor.{Props, ActorRef, Actor, ActorLogging}
import java.net.InetSocketAddress
import com.github.abulychev.iris.dht.storage.columns.{EndpointColumn, StringColumn}
import com.github.abulychev.iris.dht.storage.Row
import scala.util.Failure
import scala.concurrent.duration._
import com.github.abulychev.iris.dht.actor.DistributedHashTable
import scala.collection.mutable

/**
 * User: abulychev
 * Date: 10/23/14
 */
class ChunkRoutingActor(localAddress: InetSocketAddress,
                        dht: ActorRef) extends Actor with ActorLogging {

  import ChunkRoutingActor._

  private val ids = mutable.Set.empty[String]

  context.system.scheduler.schedule(RepublishInterval, RepublishInterval, self, Republish)(context.dispatcher)

  def receive = {
    case GetRoutes(id) =>
      val req = sender()

      context.actorOf(Props(new Actor {
        val column = StringColumn(id)

        dht ! DistributedHashTable.Get(
          TableName,
          Row(column)
        )

        def receive = {
          case DistributedHashTable.Response(rows) =>
            val endpoints = rows.map { case Row(StringColumn(_), EndpointColumn(endpoint)) => endpoint }
            req ! RoutesResponse(endpoints)
            context.stop(self)

          case failure @ Failure(_) =>
            req ! failure
            context.stop(self)
        }
      }))

    case AddRoute(id) =>
      publish(id)
      ids += id

    case Republish =>
      ids foreach publish
  }

  def publish(id: String) {
    val column = StringColumn(id)
    dht ! DistributedHashTable.Put(
      TableName,
      Row(column, EndpointColumn(localAddress)),
      TimeToLive
    )
  }
}

object ChunkRoutingActor {
  case class AddRoute(id: String)
  case class GetRoutes(id: String)
  case class RoutesResponse(routes: List[InetSocketAddress])

  private object Republish

  private val TableName = "chunk-routes"
  val TimeToLive = 30.minutes
  val RepublishInterval = 10.minutes
}
