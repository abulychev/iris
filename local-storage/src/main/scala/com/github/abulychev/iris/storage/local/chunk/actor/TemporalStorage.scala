package com.github.abulychev.iris.storage.local.chunk.actor

import java.io.File
import java.util.concurrent.atomic.AtomicLong
import org.apache.commons.io.FileUtils
import akka.actor.{Props, Actor, ActorLogging, ActorRef}
import java.util
import com.google.common.cache._
import scala.util.Failure
import scala.util.Success
import java.util.concurrent.TimeUnit
import com.github.abulychev.iris.model.{ChunkUtils, NotPersistedChunk, Chunk}

/**
 * User: abulychev
 * Date: 3/18/14
 */

class TemporalStorage(home: File,
                      storage: ActorRef) extends Actor with ActorLogging {
  import TemporalStorage._

  private val currentTemporalChunkId = new AtomicLong(0)
  
  /* TODO: just for insurance: make logic if there is no chunk */

  def chunkFile(id: String) = new File(home, id)
  
  val chunks = CacheBuilder.newBuilder()
    .maximumSize(MaximumSize)
    .expireAfterAccess(10, TimeUnit.MINUTES)
    .removalListener(new RemovalListener[String, Slice] {
      def onRemoval(removal: RemovalNotification[String, Slice]) {
        if (removal.getCause != RemovalCause.EXPLICIT) {
          val file = chunkFile(removal.getKey)
          val data = removal.getValue.get
          FileUtils.writeByteArrayToFile(file, data)
        }
      }
    })
    .build(
      new CacheLoader[String, Slice] {
        def load(id: String): Slice = {
          val file = chunkFile(id)
          new Slice(FileUtils.readFileToByteArray(file))
        }
      }
    )

  override def preStart() {
    home.mkdirs(); home.listFiles().foreach(_.delete())
  }

  def receive = {
    case CreateChunk(offset, size, data) =>
      val ndata = new Array[Byte](size)
      System.arraycopy(data, 0, ndata, 0, Math.min(size, data.length))

      val id = currentTemporalChunkId.incrementAndGet().toString
      chunks.put(id, new Slice(ndata))

      sender ! Success(NotPersistedChunk(id, offset, size))

    case WriteToChunk(chunk, data, chunkOffset) =>
      require(!chunk.persisted)

      val slice = chunks.get(chunk.id)
      slice.write(data, chunkOffset)

      sender ! Success(chunk)

    case Get(id) =>
      val slice = chunks.get(id)
      sender ! Some(slice.get)

    case Truncate(chunk, offset) =>
      require(!chunk.persisted)

      val slice = chunks.get(chunk.id)
      slice.truncate(offset)
      
      sender ! Success(NotPersistedChunk(chunk.id, chunk.offset, offset))


    case Persist(chunk) =>
      require(!chunk.persisted)

      val slice = chunks.get(chunk.id)
      chunks.invalidate(chunk.id)
      val data = slice.get
      val id = ChunkUtils.sha1(data)

      val req = sender()
      context.actorOf(Props(new Actor {
        storage ! ChunkStorage.Put(id, data)
        def receive = {
          case failure @ Failure(_) =>
            req ! failure
            context.stop(self)

          case ChunkStorage.Acknowledged =>
            req ! Success(Chunk(id, chunk.offset, chunk.size))
            val file = chunkFile(chunk.id)
            file.delete()

            context.stop(self)
        }
      }))
  }
}

object TemporalStorage {
  case class Get(id: String)
  case class CreateChunk(offset: Int, size: Int, data: Array[Byte])
  case class WriteToChunk(chunk: Chunk, data: Array[Byte], chunkOffset: Int)
  case class Truncate(chunk: Chunk, offset: Int)
  case class Persist(chunk: Chunk)

  val MaximumSize = 20
}


class Slice private(private var array: Array[Byte], private var size: Int) {
  def this(array: Array[Byte]) = this(array, array.length)
  def this(size: Int) = this(new Array[Byte](size))

  def length = size

  def get: Array[Byte] = {
    util.Arrays.copyOf(array, size)
  }

  def write(data: Array[Byte], offset: Int) {
    if (array.length < offset + data.length) shrink((offset + data.length) * 2)
    System.arraycopy(data, 0, array, offset, data.length)
    size = Math.max(size, offset + data.length)
  }

  def truncate(offset: Int) {
    if (offset > size) {
      if (array.length < offset) shrink(offset * 2)
      for (i <- size until offset) array(i) = 0.toByte
    }
    size = offset
  }

  private def shrink(length: Int) {
    val newArray = new Array[Byte](length)
    System.arraycopy(array, 0, newArray, 0, array.length)
    array = newArray
  }
}
