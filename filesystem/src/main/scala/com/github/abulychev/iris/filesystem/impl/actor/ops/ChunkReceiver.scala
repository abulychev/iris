package com.github.abulychev.iris.filesystem.impl.actor.ops

import akka.actor.{Actor, ActorRef}
import scala.util.{Failure, Success}
import com.github.abulychev.iris.storage.local.chunk.actor.{TemporalStorage, ChunkStorage}
import com.github.abulychev.iris.model.Chunk
import com.github.abulychev.iris.filesystem.Error

/**
 * User: abulychev
 * Date: 3/21/14
 */
class ChunkReceiver(chunk: Chunk,
                    storage: ActorRef,
                    temporal: ActorRef,
                    namenode: ActorRef) extends Actor {

  override def preStart() {
    if (!chunk.persisted) temporal ! TemporalStorage.Get(chunk.id)
                     else  storage ! ChunkStorage.Get(chunk.id)
  }

  def receive = {
    case Some(data) =>
      context.parent ! Success((chunk, data))
      context.stop(self)

    case ChunkStorage.Response(Some(data)) =>
      context.parent ! Success((chunk, data))
      context.stop(self)

    case _ =>
      context.parent ! Failure(Error.NoDataAvailable)
      context.stop(self)
  }
}
