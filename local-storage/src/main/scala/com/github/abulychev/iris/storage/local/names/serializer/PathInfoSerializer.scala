package com.github.abulychev.iris.storage.local.names.serializer

import java.io.{DataInputStream, DataOutputStream}
import com.github.abulychev.iris.serialize.Serializer
import com.github.abulychev.iris.model.{RemovedInfo, DirectoryInfo, FileInfo, PathInfo}

/**
 * User: abulychev
 * Date: 10/21/14
 */
object PathInfoSerializer extends Serializer[PathInfo] {
  def writeTo(info: PathInfo, out: DataOutputStream) {
    info match {
      case FileInfo(path, id, size, timestamp) =>
        out.writeByte(0)
        out.writeUTF(path)
        out.writeUTF(id)
        out.writeInt(size)
        out.writeLong(timestamp)

      case DirectoryInfo(path, timestamp) =>
        out.writeByte(1)
        out.writeUTF(path)
        out.writeLong(timestamp)

      case RemovedInfo(path, timestamp) =>
        out.writeByte(2)
        out.writeUTF(path)
        out.writeLong(timestamp)
    }
  }

  def readFrom(in: DataInputStream): PathInfo = {
    in.readByte() match {
      case 0 =>
        FileInfo(in.readUTF(), in.readUTF(), in.readInt(), in.readLong())

      case 1 =>
        DirectoryInfo(in.readUTF(), in.readLong())

      case 2 =>
        RemovedInfo(in.readUTF(), in.readLong())
    }
  }
}
