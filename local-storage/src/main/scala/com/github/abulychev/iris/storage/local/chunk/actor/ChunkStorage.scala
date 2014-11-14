package com.github.abulychev.iris.storage.local.chunk.actor

import akka.actor.{Props, ActorLogging, Actor}
import scala.collection.mutable
import com.google.common.cache._
import java.util.concurrent.TimeUnit
import org.apache.commons.io.FileUtils
import java.io.File

/**
 * User: abulychev
 * Date: 10/23/14
 */
class ChunkStorage(home: File) extends Actor with ActorLogging {
  import ChunkStorage._

  home.mkdirs()
  val persisted = mutable.Set.empty[String]
  persisted ++= home.listFiles() map { _.getName }

  val chunks = CacheBuilder.newBuilder()
    .maximumWeight(MaximumSize)
    .weigher(new Weigher[String, Array[Byte]] {
      def weigh(id: String, data: Array[Byte]): Int = {
        data.length
      }
    })
    .expireAfterAccess(10, TimeUnit.MINUTES)
    .build(
      new CacheLoader[String, Array[Byte]] {
        def load(id: String): Array[Byte] = {
          val file = new File(home, id)
          FileUtils.readFileToByteArray(file)
        }
      }
    )

  def receive = {
    case Put(id, data) =>
      val file = new File(home, id)
      FileUtils.writeByteArrayToFile(file, data)
      persisted += id
      sender ! Acknowledged

    case Get(id) =>
      val data = if (persisted.contains(id)) Some(chunks.get(id)) else None
      sender ! Response(data)

    case GetAllKeys =>
      sender ! AllKeys(persisted.toSet)
  }
}

object ChunkStorage {
  def props(home: File) = Props(classOf[ChunkStorage], home)

  case class Put(id: String, data: Array[Byte])
  case class Get(id: String)
  case class Response(result: Option[Array[Byte]])
  case object Acknowledged

  case object GetAllKeys
  case class AllKeys(keys: Set[String])

  private val MaximumSize = 10L * 1000 * 1000
}