package com.github.abulychev.iris.storage.local.info

import com.github.abulychev.iris.storage.local.info.actor.FileInfoStorage
import FileInfoStorage.Get
import java.io.{DataInputStream, DataOutputStream}
import com.github.abulychev.iris.serialize.{ListSerializer, IntSerializer, StringSerializer, Serializer}
import com.github.abulychev.iris.model.{Chunk, FileContentInfo}

/**
 * User: abulychev
 * Date: 10/23/14
 */
package object serialize {
  object GetSerializer extends Serializer[Get] {
    def writeTo(get: Get, out: DataOutputStream) {
      StringSerializer.writeTo(get.id, out)
    }

    def readFrom(in: DataInputStream): Get = {
      Get(StringSerializer.readFrom(in))
    }
  }

  object ChunkSerializer extends Serializer[Chunk] {
    def writeTo(chunk: Chunk, out: DataOutputStream) {
      StringSerializer.writeTo(chunk.id, out)
      IntSerializer.writeTo(chunk.offset, out)
      IntSerializer.writeTo(chunk.size, out)
    }

    def readFrom(in: DataInputStream): Chunk = {
      Chunk(
        StringSerializer.readFrom(in),
        IntSerializer.readFrom(in),
        IntSerializer.readFrom(in)
      )
    }
  }

  object FileContentInfoSerializer extends Serializer[FileContentInfo] {
    private val serializer = ListSerializer(ChunkSerializer)

    def writeTo(info: FileContentInfo, out: DataOutputStream) {
      IntSerializer.writeTo(info.size, out)
      serializer.writeTo(info.chunks, out)
    }

    def readFrom(in: DataInputStream): FileContentInfo = {
      FileContentInfo(
        IntSerializer.readFrom(in),
        serializer.readFrom(in)
      )
    }
  }
}
