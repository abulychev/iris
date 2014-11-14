package com.github.abulychev.iris.localfs.actor.file

import java.nio.ByteBuffer
import akka.actor.{ActorRef, Props, ActorLogging, Actor}
import scala.util.{Failure, Success}
import com.github.abulychev.iris.storage.local.chunk.actor.TemporalStorage
import TemporalStorage.{WriteToChunk, CreateChunk}
import com.github.abulychev.iris.model.{ChunkUtils, Chunk, FileContentInfo}

/**
 * User: abulychev
 * Date: 3/21/14
 */
class Writer(var info: FileContentInfo,
             buffer: ByteBuffer,
             size: Long,
             offset: Long,
             storage: ActorRef,
             temporal: ActorRef,
             namenode: ActorRef) extends Actor with ActorLogging {

  log.debug("Received new writing job")

  var result = 0

  override def preStart() {
    if (info.size < offset + size) {
      context.actorOf(Props(new Truncater(info, offset + size, storage, temporal, namenode)))
      context.become(truncating)
    } else sendLoadingRequest()
  }
 
  var chunks = List[Chunk]()
  var chunksAwait = 0
  
  private def sendLoadingRequest() {
    var chunks = List[Chunk]()
    var chunksAwait = 0

    val range = offset.toInt to (offset + size - 1).toInt
    info.chunks foreach {
      case chunk if !ChunkUtils.overlap(chunk.range, range) =>
        chunks = chunk :: chunks

      case chunk if chunk.persisted =>
        context.actorOf(Props(new ChunkReceiver(chunk, storage, temporal, namenode)))
        chunksAwait += 1

      case chunk =>
        chunks = chunk :: chunks
    }

    context.become(loading(chunksAwait, chunks))
  }
   

  private val truncating = PartialFunction[Any, Unit] {
    case Success(ninfo: FileContentInfo) =>
      info = ninfo
      sendLoadingRequest()

    case failure @ Failure(_) =>
      context.parent ! failure
      context.stop(self)
  }

  private def loading(count: Int, chunks: List[Chunk]): Receive = {
    if (count != 0) PartialFunction[Any, Unit] {
        case Success((chunk: Chunk, data: Array[Byte])) =>
          temporal ! CreateChunk(chunk.offset, data.length, data)

        //case chunk: Chunk =>
        //  context.become(loading(count - 1, chunk :: chunks))

        case Success(chunk: Chunk) =>
          context.become(loading(count - 1, chunk :: chunks))

        case failure @ Failure(e) =>
          context.parent ! failure
          context.stop(self)

    } else {
      val range = offset.toInt to (offset + size - 1).toInt

      var chunksAwait = 0
      var nchunks = List[Chunk]()

      chunks sortBy {_.offset} foreach {
        case chunk if ChunkUtils.overlap(chunk.range, range) =>
          val r = ChunkUtils.intersect(range, chunk.range)
          val data = new Array[Byte](r.size)
          buffer.get(data, 0, r.size)

          temporal ! WriteToChunk(chunk, data, r.start - chunk.offset)
          chunksAwait += 1
          result += r.size

        case chunk =>
          nchunks = chunk :: nchunks
      }

      writing(chunksAwait, nchunks)
    }

  }

  def writing(count: Int, chunks: List[Chunk]): Receive = {
    if (count == 0) {
      info = FileContentInfo(info.size, chunks sortBy { _.offset })
      context.parent ! Success((info, result))
      context.stop(self)

      PartialFunction[Any, Unit] { case _ =>  }
    } else  PartialFunction[Any, Unit] {
        case Success(chunk: Chunk) =>
          context.become(writing(count - 1, chunk :: chunks))

        case failure @ Failure(_) =>
          context.parent ! failure
          context.stop(self)
    }
  }

  def receive = { case _ => }
}
