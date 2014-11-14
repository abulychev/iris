package com.github.abulychev.iris.localfs.actor

import akka.actor.{ActorLogging, Props, ActorRef, Actor}
import net.fusejna.{DirectoryFiller, StructStat}
import net.fusejna.types.TypeMode
import net.fusejna.types.TypeMode.NodeType
import com.github.abulychev.iris.localfs.error._
import com.github.abulychev.iris.storage.local.names.actor.NameNode._
import com.github.abulychev.iris.storage.local.names.actor.NameNode.GetFileContentInfo
import scala.util.Success
import com.github.abulychev.iris.storage.local.names.actor.NameNode.GetPathInfo
import com.github.abulychev.iris.storage.local.names.actor.NameNode.GetDirectory
import com.github.abulychev.iris.storage.local.names.actor.NameNode.PutFile
import scala.util.Failure
import scala.Some
import com.github.abulychev.iris.localfs.LocalStorage
import com.github.abulychev.iris.model.{DirectoryInfo, FileInfo, FileContentInfo}

/**
 * User: abulychev
 * Date: 3/20/14
 */

class FSActor(storage: ActorRef,
              temporal: ActorRef,
              namenode: ActorRef) extends Actor with ActorLogging {

  import FSActor._

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
            context.parent ! Opened(path, opt.getOrElse(FileContentInfo(0, Nil)), req)
            context.stop(self)
        }
      }))


    case Opened(path, info, req) =>
      val file = context.actorOf(fileProps(path, info))
      req ! Success(file)

    case Create(path: String) =>
      val info = FileContentInfo(0, Nil)
      namenode ! PutFile(path, FileInfo(path, "0", 0, System.currentTimeMillis), info)

      val file = context.actorOf(fileProps(path, info))
      sender ! Success(file)

    case GetAttributes(path, stat) =>
      val req = sender

      context.actorOf(Props(new Actor {
        override def preStart() {
          namenode ! GetPathInfo(path)
        }

        def receive = PartialFunction[Any, Unit] {
          case Some(FileInfo(_, _, size, ts)) =>
            stat
              .setMode(NodeType.FILE, true, true, false)
              .size(size)
              .blksize(LocalStorage.ChunkSize)
              .setAllTimesMillis(ts)

            req ! Success(0)

          case Some(DirectoryInfo(_, _)) =>
            stat
              .setMode(NodeType.DIRECTORY, true, true, false)

            req ! Success(0)

          case _ =>
            req ! Failure(NoSuchFileOrDirectory)

        } andThen { _ => context.stop(self) }
      }))

    case ReadDirectory(path, filler) =>
      val req = sender

      context.actorOf(Props(new Actor {
        override def preStart() {
          namenode ! GetDirectory(path)
        }

        def receive = {
          case list: List[String] =>
            list.foreach(filler.add(_))
            req ! Success(0)
            context.stop(self)
        }
      }))

    case MakeDirectory(path, mode) =>
      namenode ! PutDirectory(path, DirectoryInfo(path, System.currentTimeMillis))
      sender ! Success(0)

    case _ =>
  }
}

object FSActor {
  case class Open(path: String)
  case class Create(path: String)
  case class GetAttributes(path: String, stat: StructStat.StatWrapper)
  case class ReadDirectory(path: String, filler: DirectoryFiller)
  case class MakeDirectory(path: String, mode: TypeMode.ModeWrapper)

  case class Opened(path: String, info: FileContentInfo, req: ActorRef)
}
