package com.github.abulychev.iris.distributed.names.actor

import akka.actor.{Props, ActorRef, Actor, ActorLogging}
import java.net.InetSocketAddress
import com.github.abulychev.iris.storage.local.names.NamesBlock
import scala.util.Failure
import com.github.abulychev.iris.storage.local.names.actor.NameNode
import com.github.abulychev.iris.distributed.names.{UpdateList, VersionsList}
import com.github.abulychev.iris.dht.actor.Token

/**
  * User: abulychev
  * Date: 10/21/14
  */
class SyncActor(storage: ActorRef,
                localVersions: Map[Token, Long],
                lastVersions: Map[Token, Long],
                handler: ActorRef,
                versionDht: ActorRef) extends Actor with ActorLogging {

  val versions = VersionsList(localVersions)

  override def preStart() {
    val tokens = lastVersions
      .filter { case (e, v) => localVersions.getOrElse(e, 0L) < v}
      .map { case (e, _) => e }
      .toList

    val node = util.Random.shuffle(tokens).headOption
    if (node.isEmpty) context.stop(self)
    if (node.nonEmpty) versionDht ! VersionsRoutingActor.NodeRequest(node.get)
  }

  def receive = {
    case VersionsRoutingActor.NodeResponse(endpoints) =>
      context.become(syncing(endpoints))
  }

  def syncing(endpoints: List[InetSocketAddress]): Receive = {
    if (endpoints.size == 0) context.stop(self)
    if (endpoints.size > 0) {
      val client = context.actorOf(NamesHttpClient.props(endpoints.head)) /* may be we should put logic here */
      client ! versions
    }

    PartialFunction[Any, Unit] {
      case Failure(_) =>
        context.become(syncing(endpoints.tail))

      case list @ UpdateList(updates) =>
        val block = updates.values.foldLeft(NamesBlock()){ case (a,b) => a merge b }
        storage ! NameNode.PutPaths(block.paths.values.toList)
        handler ! list

        context.stop(self)
      // TODO:
      //context.become({
      //  case NameNode.Acknowledged =>
      //    handler ! list
      //    context.stop(self)
      //})
    }
  }
}

object SyncActor {
   def props(storage: ActorRef,
             localVersions: Map[Token, Long],
             lastVersions: Map[Token, Long],
             handler: ActorRef,
             versionDht: ActorRef) =
     Props(classOf[SyncActor],
       storage,
       localVersions,
       lastVersions,
       handler,
       versionDht
     )

 }
