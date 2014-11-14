package com.github.abulychev.iris.distributed.names.actor

import akka.actor.{Props, ActorRef, Actor, ActorLogging}
import com.github.abulychev.iris.storage.local.names.actor.NameNode
import java.net.InetSocketAddress
import java.io.File
import com.github.abulychev.iris.distributed.common.actor.DistributedStorage
import com.github.abulychev.iris.dht.actor.Token

/**
  * User: abulychev
  * Date: 10/25/14
  */
class DistributedNamesStorage(home: File,
                               storage: ActorRef,
                               routing: ActorRef,
                               endpoint: InetSocketAddress,
                               token: Token,
                               prefix: String,
                               handler: ActorRef) extends Actor with ActorLogging {

  import DistributedNamesStorage._

  val controller = context.actorOf(VersionsController.props(home, storage, routing, endpoint, token, prefix))
  val http = context.actorOf(NamesHttpService.props(endpoint, controller), "http-names-service")

  handler ! DistributedStorage.RegisterHttpService(prefix, http)

  def receive = {
    case put @ NameNode.PutFile(_, _, _) =>
      forward(put)

    case put @ NameNode.PutDirectory(_, _) =>
      forward(put)

    case delete @ NameNode.DeleteFile(_) =>
      forward(delete)

    case delete @ NameNode.DeleteDirectory(_) =>
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
             prefix: String,
             handler: ActorRef): Props =
     Props(classOf[DistributedNamesStorage],
       home,
       storage,
       routing,
       endpoint,
       token,
       prefix,
       handler)


   case class VersionDiscovered(token: Token, version: Long)
   case class UpdateVersion(version: Long)
 }
