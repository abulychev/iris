package com.github.abulychev.iris.dht.serialize

import com.github.abulychev.iris.dht.actor.Storage.Get
import java.io.{DataInputStream, DataOutputStream}
import com.github.abulychev.iris.serialize.{StringSerializer, Serializer}

/**
 * User: abulychev
 * Date: 10/21/14
 */
object GetSerializer extends Serializer[Get] {
  def writeTo(get: Get, out: DataOutputStream) {
    StringSerializer.writeTo(get.tableName, out)
    RowSerializer.writeTo(get.row, out)
  }

  def readFrom(in: DataInputStream): Get = {
    Get(
      StringSerializer.readFrom(in),
      RowSerializer.readFrom(in)
    )
  }
}
