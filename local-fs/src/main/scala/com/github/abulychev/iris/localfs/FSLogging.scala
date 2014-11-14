package com.github.abulychev.iris.localfs

import net.fusejna._
import org.slf4j.LoggerFactory
import net.fusejna.types.TypeMode
import java.nio.ByteBuffer

/**
 * User: abulychev
 * Date: 3/14/14
 */
trait FSLogging extends FuseFilesystem {
  private val log = LoggerFactory.getLogger(classOf[FuseFilesystem])

  abstract override def access(path: String, access: Int): Int = {
    log.info(s"access: path=<$path> access=$access")
    super.access(path, access)
  }

  abstract override def bmap(path: String, info: StructFuseFileInfo.FileInfoWrapper): Int = {
    log.info(s"bmap: path=<$path>")
    super.bmap(path ,info)
  }

  abstract override def chmod(path: String, mode: TypeMode.ModeWrapper): Int = {
    log.info(s"chmod: path=<$path> mode=<$mode>")
    super.chmod(path, mode)
  }

  abstract override def chown(path: String, uid: Long, gid: Long): Int = {
    log.info(s"chmod: path=<$path> uid=$uid gid=$gid")
    super.chown(path, uid, gid)
  }

  abstract override def create(path: String, mode: TypeMode.ModeWrapper, info: StructFuseFileInfo.FileInfoWrapper): Int = {
    log.info(s"create: path=<$path> mode=<$mode>")
    super.create(path, mode, info)
  }

  abstract override def destroy() {
    log.info(s"destroy")
    super.destroy()
  }

  abstract override def fgetattr(path: String, stat: StructStat.StatWrapper, info: StructFuseFileInfo.FileInfoWrapper): Int = {
    log.info(s"fgetattr: path=<$path>")
    super.fgetattr(path, stat, info)
  }

  abstract override def flush(path: String, info: StructFuseFileInfo.FileInfoWrapper): Int = {
    log.info(s"flush: path=<$path>")
    super.flush(path, info)
  }

  abstract override def fsync(path: String, datasync: Int, info: StructFuseFileInfo.FileInfoWrapper): Int = {
    log.info(s"fsync: path=<$path> datasync=$datasync")
    super.fsync(path, datasync, info)
  }

  abstract override def fsyncdir(path: String, datasync: Int, info: StructFuseFileInfo.FileInfoWrapper): Int = {
    log.info(s"fsyncdir: path=<$path> datasync=$datasync")
    super.fsyncdir(path, datasync, info)
  }

  abstract override def ftruncate(path: String, offset: Long, info: StructFuseFileInfo.FileInfoWrapper): Int = {
    log.info(s"ftruncate: path=<$path> offset=$offset")
    super.ftruncate(path, offset, info)
  }

  abstract override def getattr(path: String, stat: StructStat.StatWrapper): Int = {
    log.info(s"getattr: path=<$path>")
    super.getattr(path, stat)
  }

  abstract override def getxattr(path: String, xattr: String, filler: XattrFiller, size: Long, position: Long): Int = {
    log.info(s"getxattr: path=<$path>")
    super.getxattr(path, xattr, filler, size, position)
  }

  abstract override def init() {
    log.info(s"init")
    super.init()
  }

  abstract override def link(path: String, target: String): Int = {
    log.info(s"link: path=<$path> target=<$target>")
    super.link(path, target)
  }

  abstract override def listxattr(path: String, filler: XattrListFiller): Int = {
    log.info(s"listxattr: path=<$path>")
    super.listxattr(path, filler)
  }

  abstract override def lock(path: String, info: StructFuseFileInfo.FileInfoWrapper, command: FlockCommand, flock: StructFlock.FlockWrapper): Int = {
    log.info(s"lock: path=<$path>")
    super.lock(path, info, command, flock)
  }

  abstract override def mkdir(path: String, mode: TypeMode.ModeWrapper): Int = {
    log.info(s"mkdir: path=<$path>")
    super.mkdir(path, mode)
  }

  abstract override def mknod(path: String, mode: TypeMode.ModeWrapper, dev: Long): Int = {
    log.info(s"mknod: path=<$path> mode=<$mode>")
    super.mknod(path, mode, dev)
  }

  abstract override def open(path: String, info: StructFuseFileInfo.FileInfoWrapper): Int = {
    log.info(s"open: path=<$path>")
    super.open(path, info)
  }


  abstract override def opendir(path: String, info: StructFuseFileInfo.FileInfoWrapper): Int = {
    log.info(s"opendir: path=<$path>")
    super.opendir(path, info)
  }

  abstract override def read(path: String, buffer: ByteBuffer, size: Long, offset: Long, info: StructFuseFileInfo.FileInfoWrapper): Int = {
    log.info(s"read: path=<$path> offset=$offset size=$size")
    super.read(path, buffer, size, offset, info)
  }

  abstract override def readdir(path: String, filler: DirectoryFiller): Int = {
    log.info(s"readdir: path=<$path>")
    super.readdir(path, filler)
  }

  abstract override def readlink(path: String, buffer: ByteBuffer, size: Long): Int = {
    log.info(s"readlink: path=<$path> size=$size")
    super.readlink(path, buffer, size)
  }

  abstract override def release(path: String, info: StructFuseFileInfo.FileInfoWrapper): Int = {
    log.info(s"release: path=<$path>")
    super.release(path, info)
  }

  abstract override def releasedir(path: String, info: StructFuseFileInfo.FileInfoWrapper): Int = {
    log.info(s"releasedir: path=<$path>")
    super.releasedir(path, info)
  }

  abstract override def removexattr(path: String, xattr: String): Int = {
    log.info(s"removexattr: path=<$path>")
    super.removexattr(path, xattr)
  }

  abstract override def rename(path: String, newName: String): Int = {
    log.info(s"rename: path=<$path> newName=<$newName>")
    super.rename(path, newName)
  }

  abstract override def rmdir(path: String): Int = {
    log.info(s"rmdir: path=<$path>")
    super.rmdir(path)
  }

  abstract override def setxattr(path: String, xattr: String, buf: ByteBuffer, size: Long, flags: Int, position: Int): Int = {
    log.info(s"setxattr: path=<$path> xattr=<$xattr>")
    super.setxattr(path, xattr, buf, size, flags, position)
  }

  abstract override def statfs(path: String, wrapper: StructStatvfs.StatvfsWrapper): Int = {
    log.info(s"statfs: path=<$path>")
    super.statfs(path, wrapper)
  }

  abstract override def symlink(path: String, target: String): Int = {
    log.info(s"symlink: path=<$path> target=<$target>")
    super.symlink(path, target)
  }

  abstract override def truncate(path: String, offset: Long): Int = {
    log.info(s"truncate: path=<$path> offset=$offset")
    super.truncate(path, offset)
  }

  abstract override def unlink(path: String): Int = {
    log.info(s"unlink: path=<$path>")
    super.unlink(path)
  }

  abstract override def utimens(path: String, wrapper: StructTimeBuffer.TimeBufferWrapper): Int = {
    log.info(s"utimens: path=<$path>")
    super.utimens(path, wrapper)
  }

  abstract override def write(path: String, buf: ByteBuffer, bufSize: Long, writeOffset: Long, wrapper: StructFuseFileInfo.FileInfoWrapper): Int = {
    log.info(s"write: path=<$path> size=$bufSize offset=$writeOffset")
    super.write(path, buf, bufSize, writeOffset, wrapper)
  }
}
