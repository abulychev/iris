package com.github.abulychev.iris.filesystem.impl.actor.ops

import akka.actor.{Props, ActorRef, Actor, ActorLogging}
import com.github.abulychev.iris.storage.local.chunk.actor.TemporalStorage
import TemporalStorage.{CreateChunk, Truncate}
import scala.util.{Success, Failure}
import com.github.abulychev.iris.model.{Chunk, FileContentInfo}
import com.github.abulychev.iris.storage.local.LocalStorage

/**
 * User: abulychev
 * Date: 3/21/14
 */
class Truncater(info: FileContentInfo,
                offset: Long,
                storage: ActorRef,
                temporal: ActorRef,
                namenode: ActorRef) extends Actor with ActorLogging {

  log.debug("Received new truncating job")

  val ranges = LocalStorage.createChunkRanges(offset.toInt)

  var persistedChunkRange: Range = _

  override def preStart() {
    if (info.size == offset) {
      context.parent ! Success(info)
      context.stop(self)
    }

    var chunks = List[Chunk]()
    var chunksAwait = 0

    info.chunks.zipAll(ranges, null, null) foreach {
      case (chunk, null) =>

      case (null, range) =>
        temporal ! CreateChunk(range.start, range.size, new Array[Byte](range.size))
        chunksAwait += 1

      case (chunk, range) if chunk.range == range =>
        chunks = chunk :: chunks

      case (chunk, range) if !chunk.persisted =>
        temporal ! Truncate(chunk, range.size)
        chunksAwait += 1

      case (chunk, range) =>
        context.actorOf(Props(new ChunkReceiver(chunk, storage, temporal, namenode)))
        persistedChunkRange = range
        chunksAwait += 1
    }

    context.become(awaiting(chunksAwait, chunks))
  }

  private def awaiting(count: Int, chunks: List[Chunk]): Receive = {
    if (count > 0) {
      {
        case failure @ Failure(_) =>
          context.parent ! failure
          context.stop(self)

        case Success(chunk: Chunk) =>
          context.become(awaiting(count - 1, chunk :: chunks))

        case Success((chunk: Chunk, data: Array[Byte])) =>
          temporal ! CreateChunk(chunk.offset, persistedChunkRange.size, data)
      }
    } else {
      context.parent ! Success(FileContentInfo(offset.toInt, chunks.sortBy(_.offset)))
      context.stop(self)

      { case _ => }
    }
  }

  def receive = { case _ => }
}
