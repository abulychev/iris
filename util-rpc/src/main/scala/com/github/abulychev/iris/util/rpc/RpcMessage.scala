package com.github.abulychev.iris.util.rpc

import akka.util.ByteString
import com.github.abulychev.iris.serialize.{BooleanSerializer, ByteArraySerializer, LongSerializer, Serializer}
import java.io.{DataInputStream, DataOutputStream}
import java.net.InetSocketAddress
import scala.annotation.tailrec
import scala.util.Random

/**
 * User: abulychev
 * Date: 12/17/14
 */
private[rpc] case class RpcMessage(reqid: Long, bytes: ByteString, first: Boolean, last: Boolean) {
  require((reqid == 0) == (first && last))
  require(!(reqid == 0) || first)
  require(!(reqid == 0) || last)

  require(!(reqid != 0 && first) || !last)
}

private[rpc] object RpcMessage {
  @tailrec
  def generateId(used: Set[Long]): Long = Random.nextLong() match  {
    case 0 => generateId(used)
    case x if used(x) => generateId(used)
    case x => x
  }
}


private[rpc] object RpcMessageSerializer extends Serializer[RpcMessage] {
  def writeTo(message: RpcMessage, out: DataOutputStream): Unit = {
    LongSerializer.writeTo(message.reqid, out)
    ByteArraySerializer.writeTo(message.bytes.toArray, out)
    BooleanSerializer.writeTo(message.first, out)
    BooleanSerializer.writeTo(message.last, out)
  }

  def readFrom(in: DataInputStream): RpcMessage = {
    RpcMessage (
      LongSerializer.readFrom(in),
      ByteString(ByteArraySerializer.readFrom(in)),
      BooleanSerializer.readFrom(in),
      BooleanSerializer.readFrom(in)
    )
  }
}

private[rpc] case class WriteTo(endpoint: InetSocketAddress, message: Any)

