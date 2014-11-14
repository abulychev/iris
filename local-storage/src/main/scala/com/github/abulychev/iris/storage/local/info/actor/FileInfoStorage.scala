package com.github.abulychev.iris.storage.local.info.actor

import akka.actor.{Props, ActorLogging, Actor}
import scala.collection.mutable
import com.google.common.cache.{CacheLoader, CacheBuilder}
import java.util.concurrent.TimeUnit
import java.io.File
import org.apache.commons.io.FileUtils
import com.github.abulychev.iris.storage.local.info.serialize.FileContentInfoSerializer
import com.github.abulychev.iris.model.FileContentInfo

/**
 * User: abulychev
 * Date: 10/23/14
 */
class FileInfoStorage(home: File) extends Actor with ActorLogging {
  import FileInfoStorage._

  home.mkdirs()
  val persisted = mutable.Set.empty[String]
  persisted ++= home.listFiles() map { _.getName }

  val loader = CacheBuilder.newBuilder()
    .maximumSize(MaximumSize)
    .expireAfterAccess(10, TimeUnit.MINUTES)
    .build(
      new CacheLoader[String, FileContentInfo] {
        def load(id: String): FileContentInfo = {
          val file = new File(home, id)
          val bytes = FileUtils.readFileToByteArray(file)
          FileContentInfoSerializer.fromBinary(bytes)
        }
      }
    )


  def receive = {
    case Put(id, info) =>
      val file = new File(home, id)
      val bytes = FileContentInfoSerializer.toBinary(info)
      FileUtils.writeByteArrayToFile(file, bytes)
      persisted += id
      sender ! Acknowledged

    case Get(id) =>
      val info = if (persisted.contains(id)) Some(loader.get(id)) else None
      sender ! Response(info)

    case GetAllKeys =>
      sender ! AllKeys(persisted.toSet)
  }
}

object FileInfoStorage {
  def props(home: File): Props = Props(classOf[FileInfoStorage], home)

  case class Put(id: String, info: FileContentInfo)
  case class Get(id: String)
  case class Response(result: Option[FileContentInfo])
  case object Acknowledged
  case object GetAllKeys
  case class AllKeys(keys: Set[String])

  val MaximumSize = 100

  private case object Flush /* TODO: Flushing on the disk */
}