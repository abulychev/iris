package com.github.abulychev.iris.distributed.names.actor

import akka.actor.{Props, ActorRef, Actor, ActorLogging}
import com.github.abulychev.iris.storage.local.names.actor.NameNode
import java.net.InetSocketAddress
import java.io.File
import com.github.abulychev.iris.dht.actor.Token
import com.github.abulychev.iris.{NameService, RegisterService}

/**
  * User: abulychev
  * Date: 10/25/14
  */
class DistributedNamesStorage(home: File,
                               storage: ActorRef,
                               routing: ActorRef,
                               endpoint: InetSocketAddress,
                               token: Token,
                               handler: ActorRef,
                               registry: ActorRef) extends Actor with ActorLogging {

  import DistributedNamesStorage._

  val controller = context.actorOf(VersionsController.props(home, storage, routing, endpoint, token))
  val http = context.actorOf(NamesService.props(controller), "remote-name-service-handler")

  registry ! RegisterService(NameService, http)

  def receive = {
    case put @ NameNode.PutFile(_, _, _) =>
      forward(put)

    case put @ NameNode.PutDirectory(_, _) =>
      forward(put)

    case delete @ NameNode.Delete(_) =>
      forward(delete)

    case discovered @ VersionDiscovered(_, _) =>
      controller ! discovered

    case update @ UpdateVersion(_) =>
      handler ! update

    case msg => storage forward msg
  }

  def forward(msg: Any) {
    storage forward msg
    controller ! msg
  }
}


object DistributedNamesStorage {
   def props(home: File,
             storage: ActorRef,
             routing: ActorRef,
             endpoint: InetSocketAddress,
             token: Token,
             handler: ActorRef,
             registry: ActorRef): Props =
     Props(classOf[DistributedNamesStorage],
       home,
       storage,
       routing,
       endpoint,
       token,
       handler,
       registry)


   case class VersionDiscovered(token: Token, version: Long)
   case class UpdateVersion(version: Long)
 }
