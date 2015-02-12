package com.github.abulychev.iris.filesystem.impl.actor

import akka.actor.{ActorLogging, Props, ActorRef, Actor}
import com.github.abulychev.iris.storage.local.names.actor.NameNode._
import com.github.abulychev.iris.storage.local.names.actor.NameNode.GetFileContentInfo
import scala.util.Success
import com.github.abulychev.iris.storage.local.names.actor.NameNode.GetPathInfo
import com.github.abulychev.iris.storage.local.names.actor.NameNode.GetDirectory
import com.github.abulychev.iris.storage.local.names.actor.NameNode.PutFile
import scala.util.Failure
import scala.Some
import com.github.abulychev.iris.model.{DirectoryInfo, FileInfo, FileContentInfo}
import com.github.abulychev.iris.filesystem.Filesystem._
import com.github.abulychev.iris.filesystem.Error

/**
 * User: abulychev
 * Date: 3/20/14
 */

class FilesystemHandlerActor(storage: ActorRef,
              temporal: ActorRef,
              namenode: ActorRef) extends Actor with ActorLogging {

  import FilesystemHandlerActor._

  private def fileProps(path: String, info: FileContentInfo) = Props(new FileHandlerActor(path, info, storage, temporal, namenode))

  def receive = {
    case Open(path) =>
      val req = sender()

      context.actorOf(Props(new Actor {
        override def preStart() {
          namenode ! GetFileContentInfo(path)
        }

        def receive = {
          case opt: Option[FileContentInfo] =>
            context.parent.tell(OpenedEvent(path, opt.getOrElse(FileContentInfo(0, Nil))), req)
            context.stop(self)
        }
      }))

    case OpenedEvent(path, info) =>
      val file = context.actorOf(fileProps(path, info))
      sender ! Opened(file)

    case Create(path: String) =>
      val info = FileContentInfo(0, Nil)
      namenode ! PutFile(path, FileInfo(path, "0", 0, System.currentTimeMillis), info)

      val file = context.actorOf(fileProps(path, info))
      sender ! Created(file)

    case GetAttributes(path) =>
      val req = sender()
      val name = path.drop(path.lastIndexOf('/') + 1)

      context.actorOf(Props(new Actor {
        override def preStart() {
          namenode ! GetPathInfo(path)
        }

        def receive = PartialFunction[Any, Unit] {
          case Some(FileInfo(_, _, size, ts)) =>
            req ! Attributes(List(FileEntity(name, size, ts)))

          case Some(DirectoryInfo(_, ts)) =>
            req ! Attributes(List(DirectoryEntity(name, ts)))

          case _ =>
            req ! Failure(Error.NoSuchFileOrDirectory)

        } andThen { _ => context.stop(self) }
      }))

    case ReadDirectory(path) =>
      val req = sender()

      context.actorOf(Props(new Actor {
        override def preStart() {
          namenode ! GetDirectory(path)
        }

        def receive = {
          case DirectoryResponse(files) =>
            req ! Attributes(files map {
              case file: FileInfo => FileEntity(file.name, file.size, file.timestamp)
              case dir: DirectoryInfo => DirectoryEntity(dir.name, dir.timestamp)
            })
            context.stop(self)
        }
      }))

    case MakeDirectory(path) =>
      namenode ! PutDirectory(path, DirectoryInfo(path, System.currentTimeMillis))
      sender ! Success(0)


    case Remove(path) =>
      namenode ! Delete(path)

    case _ =>
  }
}

object FilesystemHandlerActor {
  case class OpenedEvent(path: String, info: FileContentInfo)
}
