package com.github.abulychev.iris.filesystem.impl.actor

import akka.actor.{ActorLogging, Props, ActorRef, Actor}
import scala.util.{Try, Success, Failure}
import com.github.abulychev.iris.model.FileContentInfo
import com.github.abulychev.iris.filesystem.File
import com.github.abulychev.iris.filesystem.impl.actor.ops.{Writer, Reader, Truncater, Persister}

/**
 * User: abulychev
 * Date: 3/20/14
 */
class FileHandlerActor(path: String,
                       var info: FileContentInfo,
                       storage: ActorRef,
                       temporal: ActorRef,
                       namenode: ActorRef) extends Actor with ActorLogging {

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
      case dr: File.DataRead => dr
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
    case File.Read(size, offset) =>
      if (modified) {
        context.actorOf(Props(new Reader(self, info, size, offset, storage, temporal, namenode)))
        context.become(reading(sender()))
      } else {
        val req = sender()
        context.actorOf(Props(new Reader(req, info, size, offset, storage, temporal, namenode)))
      }

    case File.Truncate(offset) =>
      context.actorOf(Props(new Truncater(info, offset, storage, temporal, namenode)))
      context.become(truncating(sender()))
      modified = true

    case File.Write(bytes, offset) =>
      context.actorOf(Props(new Writer(info, bytes, bytes.length, offset, storage, temporal, namenode)))
      context.become(writing(sender()))
      modified = true

    case File.Close =>
      if (modified) {
        context.actorOf(Props(new Persister(path, info, storage, temporal, namenode)))
        context.become(closing(sender()))
      } else {
        sender ! Success(0)
        context.stop(self)
      }
  }
}