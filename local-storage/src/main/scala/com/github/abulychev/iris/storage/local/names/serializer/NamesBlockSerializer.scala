package com.github.abulychev.iris.storage.local.names.serializer

import com.github.abulychev.iris.storage.local.names.NamesBlock
import java.io.{DataInputStream, DataOutputStream}
import com.github.abulychev.iris.serialize.{ListSerializer, Serializer}

/**
 * User: abulychev
 * Date: 10/21/14
 */
object NamesBlockSerializer extends Serializer[NamesBlock] {
  private val serializer = ListSerializer(PathInfoSerializer)

  def writeTo(block: NamesBlock, out: DataOutputStream) {
    serializer.writeTo(block.paths.values.toList, out)
  }

  def readFrom(in: DataInputStream): NamesBlock = {
    NamesBlock(
      serializer.readFrom(in)
    )
  }
}
