package com.github.abulychev.iris.dht.serialize

import java.io.{DataInputStream, DataOutputStream}
import com.github.abulychev.iris.dht.storage.Row
import com.github.abulychev.iris.serialize.{ListSerializer, Serializer}

/**
 * User: abulychev
 * Date: 10/21/14
 */
object RowSerializer extends Serializer[Row] {
  private val serializer = ListSerializer(ColumnSerializer)

  def writeTo(row: Row, out: DataOutputStream) = {
    serializer.writeTo(row.columns.toList, out)
  }

  def readFrom(in: DataInputStream): Row = {
    Row(serializer.readFrom(in))
  }
}