package com.github.abulychev.iris.dht.serialize

import com.github.abulychev.iris.dht.storage.Column
import java.io.{DataInputStream, DataOutputStream}
import com.github.abulychev.iris.serialize.{ByteArraySerializer, Serializer}

/**
 * User: abulychev
 * Date: 10/23/14
 */
object ColumnSerializer extends Serializer[Column] {
  def writeTo(column: Column, out: DataOutputStream) = {
    ByteArraySerializer.writeTo(column.value, out)
  }

  def readFrom(in: DataInputStream): Column = {
    Column(ByteArraySerializer.readFrom(in))
  }
}
