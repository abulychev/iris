package com.github.abulychev.iris.serialize

import java.net.{InetAddress, InetSocketAddress}
import java.io.{DataInputStream, DataOutputStream}

/**
 * User: abulychev
 * Date: 9/26/14
 */
object EndpointSerializer extends Serializer[InetSocketAddress] {
  def writeTo(endpoint: InetSocketAddress, out: DataOutputStream): Unit = {
    val address = endpoint.getAddress
    val buf = address.getAddress
    out.writeByte(buf.length)
    out.write(buf)
    out.writeInt(endpoint.getPort)
  }

  def readFrom(in: DataInputStream): InetSocketAddress = {
    val bytes = new Array[Byte](in.readByte)
    in.readFully(bytes, 0, bytes.length)
    val address = InetAddress.getByAddress(bytes)
    val port = in.readInt()
    new InetSocketAddress(address, port)
  }
}
