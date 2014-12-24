package com.github.abulychev.iris.dht.serialize

import com.github.abulychev.iris.serialize.{ListSerializer, Serializer}
import java.io.{DataInputStream, DataOutputStream}
import com.github.abulychev.iris.dht.actor.Storage

/**
 * User: abulychev
 * Date: 12/22/14
 */
object MessageSerializer extends Serializer[Any] {
  def writeTo(msg: Any, out: DataOutputStream) {
    msg match {
      case Storage.PutBatch(batch) =>
        out.writeByte(PutOpcode)
        ListSerializer(PutSerializer).writeTo(batch, out)

      case get @ Storage.Get(_, _) =>
        out.writeByte(GetOpcode)
        GetSerializer.writeTo(get, out)

      case resp @ Storage.Response(_) =>
        out.writeByte(ResponseOpcode)
        ResponseSerializer.writeTo(resp, out)
    }
  }

  def readFrom(in: DataInputStream): Any = {
    in.readByte() match {
      case PutOpcode => Storage.PutBatch(ListSerializer(PutSerializer).readFrom(in))
      case GetOpcode => GetSerializer.readFrom(in)
      case ResponseOpcode => ResponseSerializer.readFrom(in)
    }
  }

  val PutOpcode: Byte = 0
  val GetOpcode: Byte = 1
  val ResponseOpcode: Byte = 2
}
