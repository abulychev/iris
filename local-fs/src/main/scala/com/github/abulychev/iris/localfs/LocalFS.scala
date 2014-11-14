package com.github.abulychev.iris.localfs

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong
import net.fusejna.types.TypeMode
import net.fusejna._
import net.fusejna.util.FuseFilesystemAdapterFull
import scala.util.Try
import scala.collection._
import akka.actor.ActorRef
import com.github.abulychev.iris.localfs.actor.FSActor._
import akka.pattern.ask
import scala.concurrent._
import scala.concurrent.duration._
import com.github.abulychev.iris.localfs.actor.FileHandlerActor.{Truncate, Write, Close, Read}
import akka.util.Timeout
import com.github.abulychev.iris.localfs.actor.FSActor.Open
import com.github.abulychev.iris.localfs.actor.FSActor.GetAttributes
import scala.util.Failure
import com.github.abulychev.iris.localfs.actor.FSActor.Create
import scala.util.Success
import com.github.abulychev.iris.localfs.actor.FSActor.ReadDirectory
import com.github.abulychev.iris.localfs.error._
import com.github.abulychev.iris.storage.local.names.actor.NameNode.{DeleteDirectory, DeleteFile}
import ExecutionContext.Implicits.global

/**
 * User: abulychev
 * Date: 3/5/14
 */
class LocalFS(fsActor: ActorRef, namenode: ActorRef) extends FuseFilesystemAdapterFull {
  private implicit val timeout: Timeout = 1 day

  private val lastFileHandler = new AtomicLong(0)
  private val actorMap = new mutable.HashMap[Long, ActorRef] with mutable.SynchronizedMap[Long, ActorRef]

  // You can put it in fuse_conn_info
  // Fixes truncate after open
  override def getOptions: Array[String] = List("-oatomic_o_trunc", "-obig_writes").toArray

  override def getattr(path: String, stat: StructStat.StatWrapper): Int = {
    val r = fsActor ? GetAttributes(path, stat)
    getResult(wait[Try[Int]](r))
  }

  override def readdir(path: String, filler: DirectoryFiller): Int = {
    val r = fsActor ? ReadDirectory(path, filler)
    getResult(wait[Try[Int]](r))
  }

  override def mkdir(path: String, mode: TypeMode.ModeWrapper): Int = {
    val r = fsActor ? MakeDirectory(path, mode)
    getResult(wait[Try[Int]](r))
  }

  override def open(path: String, info: StructFuseFileInfo.FileInfoWrapper): Int = {
    val fh = lastFileHandler.incrementAndGet()
    info.fh(fh)

    val result: Try[ActorRef] = wait {
      if (info.truncate) fsActor ? Create(path) else fsActor ? Open(path)
    }

    result match {
      case Success(file) =>
        actorMap += fh -> file
        0
      case Failure(e) =>
        -ErrorCodes.EBADF
    }
  }

  override def read(path: String, buffer: ByteBuffer, size: Long, offset: Long, info: StructFuseFileInfo.FileInfoWrapper): Int = {
    val fh = info.fh
    val file = actorMap(fh)
    getResult(wait(file ? Read(buffer, size, offset)))
  }

  override def symlink(path: String, target: String): Int = {
    /*
    val file = new File(path)

    if (!file.exists) return -ErrorCodes.ENOENT()

    val length = file.length.toInt
    val chunks = storage.addFile(file).get
    val info = FileContentInfo(length, chunks)

    val result = names.putFileContentInfo(target, info)

    if (result.isSuccess) 0 else -ErrorCodes.EBADF
    */
    0
  }

  override def create(path: String, mode: TypeMode.ModeWrapper, info: StructFuseFileInfo.FileInfoWrapper): Int = {
    val fh = lastFileHandler.incrementAndGet()
    info.fh(fh)

    val file: Try[ActorRef] = wait(fsActor ? Create(path))
    actorMap += fh -> file.get
    0
  }

  override def write(path: String, buffer: ByteBuffer, size: Long, offset: Long, info: StructFuseFileInfo.FileInfoWrapper): Int = {
    val file = actorMap(info.fh)
    getResult(wait(file ? Write(buffer, size, offset)))
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
    namenode ! DeleteFile(path)
    0
  }

  override def rmdir(path: String): Int = {
    namenode ! DeleteDirectory(path)
    0
  }

  def wait[T](future: Future[Any]): T = {
    Await.result(future, Duration.Inf).asInstanceOf[T]
    //Await.result(future.mapTo[T], Duration.Inf)
  }

  private def getResult(value: Try[Int]): Int =
    value match {
      case Success(result) => result
      case Failure(e: ErrorCode) => -e.code
    }
}

