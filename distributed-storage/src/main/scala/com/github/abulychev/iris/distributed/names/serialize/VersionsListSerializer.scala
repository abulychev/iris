package com.github.abulychev.iris.distributed.names.serialize

import java.io.{DataInputStream, DataOutputStream}
import com.github.abulychev.iris.distributed.names.VersionsList
import com.github.abulychev.iris.serialize.{LongSerializer, MapSerializer, Serializer}
import com.github.abulychev.iris.dht.serialize.TokenSerializer

/**
 * User: abulychev
 * Date: 10/21/14
 */
object VersionsListSerializer extends Serializer[VersionsList] {
  private val serializer = MapSerializer(
    TokenSerializer,
    LongSerializer
  )

  def writeTo(list: VersionsList, out: DataOutputStream) = {
    serializer.writeTo(list.versions, out)
  }

  def readFrom(in: DataInputStream): VersionsList = {
    VersionsList(serializer.readFrom(in))
  }
}
