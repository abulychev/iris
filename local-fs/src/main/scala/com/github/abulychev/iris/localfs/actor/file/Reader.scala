package com.github.abulychev.iris.localfs.actor.file

import akka.actor.{ActorRef, Props, Actor, ActorLogging}
import java.nio.ByteBuffer
import scala.util.{Failure, Success}
import com.github.abulychev.iris.model.{ChunkUtils, Chunk, FileContentInfo}

/**
 * User: abulychev
 * Date: 3/21/14
 */
class Reader(req: ActorRef,
             info: FileContentInfo,
             buffer: ByteBuffer,
             size: Long,
             offset: Long,
             storage: ActorRef,
             temporal: ActorRef,
             namenode: ActorRef) extends Actor with ActorLogging {

  log.debug("Received new reading job")

  private val range = offset.toInt to (offset + size - 1).toInt
  private val chunksToRead = info.findChunks(range)

  override def preStart() {
    chunksToRead foreach { case chunk =>
      context.actorOf(Props(new ChunkReceiver(chunk, storage, temporal, namenode)))
    }
  }

  def receive = await(chunksToRead.size)

  private var list = List[(Chunk, Array[Byte])]()

  def await(count: Int): Receive = {
    if (count == 0) {
      var result = 0
      val sorted = list sortBy { case (chunk, _) => chunk.offset }
      sorted foreach { case (Chunk(_, offset, size), data) =>
        val r = ChunkUtils.intersect(range, offset to (offset + size - 1))
        buffer.put(data, r.start - offset, r.size)
        result += r.size
      }

      req ! Success(result)
      context.stop(self)
      PartialFunction[Any, Unit] { case _ => }
    } else PartialFunction[Any, Unit] {
      case Success((chunk: Chunk, data: Array[Byte])) =>
        list = (chunk, data) :: list
        context.become(await(count - 1))

      case failure @ Failure(e) =>
        req ! failure
        context.stop(self)
    }
  }
}
