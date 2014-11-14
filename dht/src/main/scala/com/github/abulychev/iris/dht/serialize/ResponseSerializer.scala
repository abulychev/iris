package com.github.abulychev.iris.dht.serialize

import com.github.abulychev.iris.dht.actor.Storage.Response
import java.io.{DataInputStream, DataOutputStream}
import com.github.abulychev.iris.serialize.{ListSerializer, Serializer}

/**
 * User: abulychev
 * Date: 10/21/14
 */
object ResponseSerializer extends Serializer[Response] {
  private val serializer = ListSerializer(RowSerializer)

  def writeTo(response: Response, out: DataOutputStream) {
    serializer.writeTo(response.rows, out)
  }

  def readFrom(in: DataInputStream): Response = {
    Response(serializer.readFrom(in))
  }
}
