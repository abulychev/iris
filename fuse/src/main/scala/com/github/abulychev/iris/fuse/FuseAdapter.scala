package com.github.abulychev.iris.fuse

import net.fusejna.types.TypeMode.NodeType
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong
import net.fusejna.types.TypeMode
import net.fusejna._
import net.fusejna.util.FuseFilesystemAdapterFull
import scala.util.Try
import scala.collection._
import akka.actor.ActorRef
import akka.pattern.ask
import scala.concurrent._
import scala.concurrent.duration._
import akka.util.Timeout
import scala.util.Failure
import scala.util.Success
import ExecutionContext.Implicits.global
import com.github.abulychev.iris.filesystem.Filesystem._
import com.github.abulychev.iris.filesystem.File._
import com.github.abulychev.iris.filesystem.{File, Error}
import com.github.abulychev.iris.storage.local.LocalStorage

/**
 * User: abulychev
 * Date: 3/5/14
 */
class FuseAdapter(fsActor: ActorRef) extends FuseFilesystemAdapterFull {
  private implicit val timeout: Timeout = 1 day

  private val lastFileHandler = new AtomicLong(0)
  private val actorMap = new mutable.HashMap[Long, ActorRef] with mutable.SynchronizedMap[Long, ActorRef]

  // You can put it in fuse_conn_info
  // Fixes truncate after open
  override def getOptions: Array[String] = List("-oatomic_o_trunc", "-obig_writes").toArray

  override def getattr(path: String, stat: StructStat.StatWrapper): Int = {
    val r: Any = wait(fsActor ? GetAttributes(path))

    r match {
      case Attributes(List(FileEntity(_, size, ct))) =>
        stat
          .setMode(NodeType.FILE, true, true, false)
          .size(size)
          .blksize(LocalStorage.ChunkSize)    // TODO: Remove this
          .setAllTimesMillis(ct)
        0

      case Attributes(List(DirectoryEntity(_, _))) =>
        stat
          .setMode(NodeType.DIRECTORY, true, true, false)
        0

      case _ =>  getResult(Failure(Error.NoSuchFileOrDirectory))
    }
  }

  override def readdir(path: String, filler: DirectoryFiller): Int = {
    val r = wait[Attributes](fsActor ? ReadDirectory(path))
    r.entities foreach { entity => filler.add(entity.name)}
    0
  }

  override def mkdir(path: String, mode: TypeMode.ModeWrapper): Int = {
    val r = fsActor ? MakeDirectory(path)
    getResult(wait[Try[Int]](r))
  }

  override def open(path: String, info: StructFuseFileInfo.FileInfoWrapper): Int = {
    val fh = lastFileHandler.incrementAndGet()
    info.fh(fh)

    val result: Any = wait {
      if (info.truncate) fsActor ? Create(path) else fsActor ? Open(path)
    }

    result match {
      case Opened(file) =>
        actorMap += fh -> file
        0

      case Created(file) =>
        actorMap += fh -> file
        0

      case _ => -ErrorCodes.EBADF
    }
  }

  override def read(path: String, buffer: ByteBuffer, size: Long, offset: Long, info: StructFuseFileInfo.FileInfoWrapper): Int = {
    val fh = info.fh
    val file = actorMap(fh)
    val r: Any = wait(file ? File.Read(size, offset))

    r match {
      case File.DataRead(bytes) =>
        buffer.put(bytes)
        bytes.length

      case Failure(e) =>
        getResult(Failure(e))
    }
  }

  override def create(path: String, mode: TypeMode.ModeWrapper, info: StructFuseFileInfo.FileInfoWrapper): Int = {
    val fh = lastFileHandler.incrementAndGet()
    info.fh(fh)

    val Created(file) = wait[Created](fsActor ? Create(path))
    actorMap += fh -> file
    0
  }

  override def write(path: String, buffer: ByteBuffer, size: Long, offset: Long, info: StructFuseFileInfo.FileInfoWrapper): Int = {
    val file = actorMap(info.fh)
    val bytes = new Array[Byte](size.toInt)
    buffer.get(bytes, 0 , size.toInt)

    getResult(wait(file ? File.Write(bytes, offset)))
  }

  override def truncate(path: String, offset: Long): Int = {
    val result = for {
      r1 <- (fsActor ? Open(path)).mapTo[Try[ActorRef]]
      file = r1.get
      r2 <- file ? Truncate(offset)
      r3 <- file ? Close
    } yield r2

    getResult(wait[Try[Int]](result))
  }

  override def ftruncate(path: String, offset: Long, info: StructFuseFileInfo.FileInfoWrapper): Int = {
    val file = actorMap(info.fh)
    getResult(wait[Try[Int]](file ? Truncate(offset)))
  }


  override def release(path: String, info: StructFuseFileInfo.FileInfoWrapper): Int = {
    val file = actorMap(info.fh)
    val r = file ? Close
    getResult(wait[Try[Int]](r))
  }

  override def unlink(path: String): Int = {
    fsActor ! Remove(path)
    0
  }

  override def rmdir(path: String): Int = {
    fsActor ! Remove(path)
    0
  }

  def wait[T](future: Future[Any]): T = {
    Await.result(future, Duration.Inf).asInstanceOf[T]
    //Await.result(future.mapTo[T], Duration.Inf)
  }

  private def getResult(value: Try[Int]): Int =
    value match {
      case Success(result) => result
      case Failure(Error.NoSuchFileOrDirectory) => -ErrorCodes.ENOENT
      case Failure(Error.ResourceTemporarilyUnavailable) => -ErrorCodes.EAGAIN
      case Failure(Error.FileExists) => -ErrorCodes.EEXIST
      case Failure(Error.NoDataAvailable) => -ErrorCodes.ENODATA
      case Failure(_) => -1
    }
}

