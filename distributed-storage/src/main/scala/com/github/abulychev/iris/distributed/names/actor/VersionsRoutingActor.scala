package com.github.abulychev.iris.distributed.names.actor

import akka.actor.{ActorLogging, Props, Actor, ActorRef}
import java.net.InetSocketAddress
import com.github.abulychev.iris.dht.actor.{Token, DistributedHashTable}
import com.github.abulychev.iris.dht.storage.columns.{LongColumn, EndpointColumn}
import com.github.abulychev.iris.dht.storage.{Column, Row}
import scala.util.Failure
import scala.collection.mutable
import scala.concurrent.duration._

/**
 * User: abulychev
 * Date: 10/20/14
 */
class VersionsRoutingActor(localAddress: InetSocketAddress, dht: ActorRef) extends Actor with ActorLogging {
  import VersionsRoutingActor._

  private val versions = mutable.Map.empty[Token, Long]

  context.system.scheduler.schedule(RepublishInterval, RepublishInterval, self, Republish)(context.dispatcher)

  def receive = {
    case NodeRequest(token) =>
      val req = sender()

      context.actorOf(Props(new Actor {
        val column = Column(token.value)

        dht ! DistributedHashTable.Get(
          Token(column.value),
          TableName,
          Row(column)
        )

        def receive = {
          case DistributedHashTable.Response(rows) =>
            val list = rows
              .map { case Row(Column(_), LongColumn(version), EndpointColumn(ep)) => (ep, version) }
              .sortBy { case (_, v) => v }
              .map { case (e, _) => e }
              .distinct

            req ! NodeResponse(list)
            context.stop(self)

          case failure @ Failure(_) =>
            req ! failure
            context.stop(self)
        }
      }))

    case UpdateVersion(token, version) =>
      publish(token, version)
      if (versions.getOrElse(token, 0L) < version) {
        versions += token -> version
      }

    case Republish =>
      /* TODO: batching */
      versions foreach { case (e, v) => publish(e, v) }

    case msg => log.error(s"Unhandled message: {$msg}")
  }

  def publish(token: Token, version: Long) = {
    val column = Column(token.value)
    dht ! DistributedHashTable.Put(
      TableName,
      Row(column, LongColumn(version), EndpointColumn(localAddress)),
      TimeToLive
    )
  }
}

object VersionsRoutingActor {
  case class NodeRequest(token: Token)
  case class NodeResponse(endpoints: List[InetSocketAddress])
  case class UpdateVersion(token: Token, version: Long)

  private object Republish

  val TableName = "versions"
  val TimeToLive = 6.minutes
  val RepublishInterval = TimeToLive / 2
}
