package com.github.abulychev.iris.filesystem.impl.actor.ops

import akka.actor.{ActorRef, Props, Actor, ActorLogging}
import scala.util.{Failure, Success}
import com.github.abulychev.iris.model.{ChunkUtils, Chunk, FileContentInfo}
import com.github.abulychev.iris.filesystem.File

/**
 * User: abulychev
 * Date: 3/21/14
 */
class Reader(req: ActorRef,
             info: FileContentInfo,
             size: Long,
             offset: Long,
             storage: ActorRef,
             temporal: ActorRef,
             namenode: ActorRef) extends Actor with ActorLogging {

  log.debug("Received new reading job")

  private val range = offset.toInt to Math.min((offset + size - 1).toInt, info.size - 1)
  private val chunksToRead = info.findChunks(range)
  private val result = new Array[Byte](range.length)

  override def preStart() {
    chunksToRead foreach { case chunk =>
      context.actorOf(Props(new ChunkReceiver(chunk, storage, temporal, namenode)))
    }
  }

  def receive = await(chunksToRead.size)

  private var list = List[(Chunk, Array[Byte])]()

  def await(count: Int): Receive = {
    if (count == 0) {
      val sorted = list sortBy { case (chunk, _) => chunk.offset }
      sorted foreach { case (Chunk(_, offset, size), data) =>
        val r = ChunkUtils.intersect(range, offset to (offset + size - 1))
        System.arraycopy(data, r.start - offset, result, (r.start - this.offset).toInt, r.size)
      }

      req ! File.DataRead(result)
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
