package com.github.abulychev.iris.localfs.actor

import akka.actor.{ActorLogging, Props, ActorRef, Actor}
import scala.util.{Try, Success, Failure}
import java.nio.ByteBuffer
import com.github.abulychev.iris.localfs.actor.file.{Persister, Writer, Truncater, Reader}
import com.github.abulychev.iris.model.FileContentInfo

/**
 * User: abulychev
 * Date: 3/20/14
 */
class FileHandlerActor(path: String,
                       var info: FileContentInfo,
                       storage: ActorRef,
                       temporal: ActorRef,
                       namenode: ActorRef) extends Actor with ActorLogging {

  import FileHandlerActor._

  private var modified = false
  private var queue = List[(ActorRef, Any)]()

  private val PutInQueue = PartialFunction[Any, Unit] {
    case msg => queue = queue ++ List((sender, msg))
  }

  private def sendResponse(req: ActorRef) = PartialFunction[Any, Unit] {
    case result =>
      req ! result
      context.become(receive)
      deq()
  }

  private def reading(req: ActorRef): Receive = {
    val pf: PartialFunction[Any, Any] = {
      case result: Try[_] => result
    }
    pf andThen sendResponse(req) orElse PutInQueue
  }

  private def truncating(req: ActorRef) = {
    val pf: PartialFunction[Any, Any] = {
      case Success(ninfo: FileContentInfo) =>
        info = ninfo
        Success(0)
      case failure @ Failure(_) => failure
    }
    pf andThen sendResponse(req) orElse PutInQueue
  }

  private def writing(req: ActorRef) = {
    val pf: PartialFunction[Any, Any] = {
      case Success((ninfo: FileContentInfo, result: Int)) =>
        info = ninfo
        Success(result)
      case failure @ Failure(_) => failure
    }
    pf andThen sendResponse(req) orElse PutInQueue
  }

  private def closing(req: ActorRef) = {
    val pf: PartialFunction[Any, Any] = {
      case Success(_) =>
        context.stop(self)
        Success(0)
      case failure @ Failure(_) => failure
    }
    pf andThen sendResponse(req) orElse PutInQueue
  }

  private def deq() {
    queue foreach { case (sender, msg) => self.tell(msg, sender) }
    queue = List()
  }

  def receive: Receive = {
    case Read(buffer, size, offset) =>
      if (modified) {
        context.actorOf(Props(new Reader(self, info, buffer, size, offset, storage, temporal, namenode)))
        context.become(reading(sender()))
      } else {
        val req = sender()
        context.actorOf(Props(new Reader(req, info, buffer, size, offset, storage, temporal, namenode)))
      }

    case Truncate(offset) =>
      context.actorOf(Props(new Truncater(info, offset, storage, temporal, namenode)))
      context.become(truncating(sender()))
      modified = true

    case Write(buffer, size, offset) =>
      context.actorOf(Props(new Writer(info, buffer, size, offset, storage, temporal, namenode)))
      context.become(writing(sender()))
      modified = true

    case Close =>
      if (modified) {
        context.actorOf(Props(new Persister(path, info, storage, temporal, namenode)))
        context.become(closing(sender()))
      } else {
        sender ! Success(0)
        context.stop(self)
      }
  }
}

object FileHandlerActor {
  case class Read(buffer: ByteBuffer, size: Long, offset: Long)
  case class Write(buffer: ByteBuffer, size: Long, offset: Long)
  case class Truncate(offset: Long)
  object Close
}
