package com.github.abulychev.iris.storage.local.names.actor

import akka.actor._
import com.github.abulychev.iris.storage.local.names.NamesStorage
import java.io.File
import com.github.abulychev.iris.storage.local.info.actor.FileInfoStorage
import scala.Some
import com.github.abulychev.iris.model._
import com.github.abulychev.iris.model.RemovedInfo
import scala.Some
import com.github.abulychev.iris.model.FileContentInfo

/**
 * User: abulychev
 * Date: 3/31/14
 */
class NameNode(home: File,
               infoStorage: ActorRef) extends Actor with ActorLogging {

  import NameNode._

  private val storage = new NamesStorage(home)
  private def fs = storage.getPathInfo

  def receive = {
    case PutFile(path, fileInfo, contentInfo) =>
      require(path == fileInfo.path)
      require(fileInfo.size == contentInfo.size)

      storage.put(fileInfo)
      infoStorage ! FileInfoStorage.Put(fileInfo.id, contentInfo)
      //sender ! Acknowledged

    case PutDirectory(path, directoryInfo) =>
      require(path == directoryInfo.path)

      storage.put(directoryInfo)
      //sender ! Acknowledged

    case DeleteFile(path) =>
      val removedInfo = RemovedInfo(path, System.currentTimeMillis)
      storage.put(removedInfo)
      //sender ! Acknowledged

    case DeleteDirectory(path) =>
      val removedInfo = RemovedInfo(path, System.currentTimeMillis)
      storage.put(removedInfo)
      //sender ! Acknowledged

    case PutPaths(paths) =>
      storage.put(paths)
      //sender ! Acknowledged

    case GetPathInfo(path) =>
      sender ! fs.get(path)

    case GetFileContentInfo(path) =>
      /* TODO: remove it */
      val req = sender()

      fs.get(path) match {
        case Some(FileInfo(_, id, _, _)) =>
          context.actorOf(Props(new Actor {
            infoStorage ! FileInfoStorage.Get(id)
            def receive = {
              case FileInfoStorage.Response(some @ Some(_)) => req ! some
              case _ => req ! None

            }
          }))

        case _ => req ! None
      }

    case GetDirectory(path) =>
      val tokens = PathInfoUtils.getTokens(path)

      val filenames = fs.values.iterator
        .filter {
          case RemovedInfo(_, _) => false
          case _ => true
        }
        .filter { case file => file.tokens.startsWith(tokens)}
        .map { case file => file.tokens.drop(tokens.size) }
        .flatMap {
          case filename :: Nil => Some(filename)
          case _ => None
        }
        .toList

      sender ! filenames

    case msg => log.info("Uncaught message: {}", msg)
  }
}

object NameNode {
  def props(home: File, infoStorage: ActorRef) = Props(classOf[NameNode], home, infoStorage)

  case class PutFile(path: String, fileInfo: FileInfo, contentInfo: FileContentInfo)
  case class PutDirectory(path: String, directoryInfo: DirectoryInfo)
  case class DeleteFile(path: String)
  case class DeleteDirectory(path: String)
  case class GetPathInfo(path: String)
  case class GetFileContentInfo(path: String)
  case class GetDirectory(path: String)

  case class PutPaths(paths: List[PathInfo])
  case object Acknowledged
}
