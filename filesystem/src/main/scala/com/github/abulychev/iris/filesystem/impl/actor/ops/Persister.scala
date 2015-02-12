package com.github.abulychev.iris.filesystem.impl.actor.ops

import akka.actor.{ActorRef, ActorLogging, Actor}
import com.github.abulychev.iris.storage.local.chunk.actor.TemporalStorage
import TemporalStorage.Persist
import scala.util.{Failure, Success}
import com.github.abulychev.iris.storage.local.names.actor.NameNode.PutFile
import com.github.abulychev.iris.model.{FileInfo, Chunk, FileContentInfo}

/**
 * User: abulychev
 * Date: 3/21/14
 */
class Persister(path: String,
                info: FileContentInfo,
                storage: ActorRef,
                temporal: ActorRef,
                namenode: ActorRef) extends Actor with ActorLogging {

  log.debug("Received new persisting job")

  override def preStart() {
    var chunksToPersist = 0
    var chunks = List[Chunk]()

    info.chunks foreach {
      case chunk =>
        if (!chunk.persisted) {
          temporal ! Persist(chunk)
          chunksToPersist += 1
        } else chunks = chunk :: chunks
    }

    context.become(persisting(chunksToPersist, chunks))
  }

  def persisting(chunksToPersist: Int, chunks: List[Chunk]): Receive = {
    if (chunksToPersist != 0) PartialFunction[Any, Unit] {
      case Success(chunk: Chunk) =>
        context.become(persisting(chunksToPersist - 1, chunk :: chunks))
      case failure @ Failure(_) =>
        context.parent ! failure
        context.stop(self)
    } else {
      val ninfo = FileContentInfo(info.size, chunks sortBy {_.offset})

      namenode ! PutFile(path, FileInfo(path, FileInfo.computeId(ninfo), ninfo.size, System.currentTimeMillis), ninfo)
      context.parent ! Success(ninfo)
      context.stop(self)

      PartialFunction[Any, Unit] { case _ =>  }
    }
  }

  def receive = { case _ => }
}
