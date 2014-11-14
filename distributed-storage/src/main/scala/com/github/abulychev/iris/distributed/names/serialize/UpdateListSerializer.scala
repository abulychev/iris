package com.github.abulychev.iris.distributed.names.serialize

import java.io.{DataInputStream, DataOutputStream}
import com.github.abulychev.iris.storage.local.names.serializer.NamesBlockSerializer
import com.github.abulychev.iris.serialize._
import com.github.abulychev.iris.distributed.names.UpdateList
import com.github.abulychev.iris.dht.serialize.TokenSerializer

/**
 * User: abulychev
 * Date: 10/21/14
 */
object UpdateListSerializer extends Serializer[UpdateList] {
  private val serializer = MapSerializer(
    TupleSerializer(
      TokenSerializer,
      LongSerializer
    ),
    NamesBlockSerializer
  )

  def writeTo(list: UpdateList, out: DataOutputStream) = {
    serializer.writeTo(list.updates, out)
  }

  def readFrom(in: DataInputStream): UpdateList = {
    UpdateList(serializer.readFrom(in))
  }
}
