package com.github.abulychev.iris.dht.serialize

import com.github.abulychev.iris.dht.actor.Storage.Put
import java.io.{DataInputStream, DataOutputStream}
import com.github.abulychev.iris.serialize.{LongSerializer, StringSerializer, Serializer}

/**
 * User: abulychev
 * Date: 10/21/14
 */
object PutSerializer extends Serializer[Put] {
  def writeTo(put: Put, out: DataOutputStream) {
    StringSerializer.writeTo(put.tableName, out)
    RowSerializer.writeTo(put.row, out)
    LongSerializer.writeTo(put.expireTime, out)
  }

  def readFrom(in: DataInputStream): Put = {
    Put(
      StringSerializer.readFrom(in),
      RowSerializer.readFrom(in),
      LongSerializer.readFrom(in)
    )
  }


}
