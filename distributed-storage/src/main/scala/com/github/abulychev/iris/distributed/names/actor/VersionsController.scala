package com.github.abulychev.iris.distributed.names.actor

import akka.actor.{ActorRef, Props, Actor, ActorLogging}
import com.github.abulychev.iris.storage.local.names.actor.NameNode
import com.github.abulychev.iris.storage.local.names.NamesBlock
import com.github.abulychev.iris.storage.local.names.actor.NameNode.{DeleteDirectory, DeleteFile, PutDirectory, PutFile}
import scala.collection.mutable
import java.net.InetSocketAddress
import scala.concurrent.duration._
import java.io.File
import com.github.abulychev.iris.distributed.names.{UpdateList, VersionsList, VersionedNamesStorage}
import com.github.abulychev.iris.distributed.names.actor.DistributedNamesStorage.VersionDiscovered
import com.github.abulychev.iris.dht.actor.Token
import com.github.abulychev.iris.model.{RemovedInfo, PathInfo}

/**
  * User: abulychev
  * Date: 10/25/14
  */
class VersionsController(home: File,
                         storage: ActorRef,
                         routing: ActorRef,
                         endpoint: InetSocketAddress,
                         token: Token,
                         prefix: String) extends Actor with ActorLogging {
  import VersionsController._

  private val versionsStorage = new VersionedNamesStorage(home)
  private val currentVersions = mutable.Map.empty[Token, Long]

  var localVersion: Long = versionsStorage.versionOf(token).getOrElse(0L)

  override def preStart()  {
    versionsStorage.lastVersions.versions.foreach { case (t, v) =>
      routing ! VersionsRoutingActor.UpdateVersion(t, v)
    }

    context.parent ! DistributedNamesStorage.UpdateVersion(localVersion)

    context.system.scheduler.schedule(5 seconds, 5 seconds, self, Tick)(context.dispatcher)
  }

  def receive = {
    case PutFile(path, fileInfo, contentInfo) =>
      require(path == fileInfo.path)
      require(fileInfo.size == contentInfo.size)

      incrementVersion(fileInfo)

    case PutDirectory(path, directoryInfo) =>
      require(path == directoryInfo.path)

      incrementVersion(directoryInfo)

    case DeleteFile(path) =>
      val removedInfo = RemovedInfo(path, System.currentTimeMillis)
      incrementVersion(removedInfo)

    case DeleteDirectory(path) =>
      val removedInfo = RemovedInfo(path, System.currentTimeMillis)
      incrementVersion(removedInfo)

    case Tick =>
      /* Sync Job */
      context.actorOf(SyncActor.props(storage, versionsStorage.lastVersions.versions, currentVersions.toMap, self, routing))

    case versions @ VersionsList(_) =>
      sender ! versionsStorage.get(versions)

    case updates @ UpdateList(_) =>
      versionsStorage.put(updates)
      updates.updates.keys foreach { case (e, v) =>
        routing ! VersionsRoutingActor.UpdateVersion(e, v)
      }

      val block = updates.updates.values.foldLeft(NamesBlock()){ case (a,b) => a merge b }
      storage ! NameNode.PutPaths(block.paths.values.toList)

    case VersionDiscovered(token, version) =>
      currentVersions += token -> version

    case msg => log.info("Uncaught message: {}", msg)
  }

  def incrementVersion(info: PathInfo): Unit = {
    localVersion += 1
    versionsStorage.put(token, localVersion, info)
    routing ! VersionsRoutingActor.UpdateVersion(token, localVersion)
    context.parent ! DistributedNamesStorage.UpdateVersion(localVersion)
  }
}


object VersionsController {
   def props(home: File,
             storage: ActorRef,
             routing: ActorRef,
             endpoint: InetSocketAddress,
             token: Token,
             prefix: String): Props =
     Props(classOf[VersionsController],
       home,
       storage,
       routing,
       endpoint,
       token,
       prefix)

   case object Tick
 }