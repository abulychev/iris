package com.github.abulychev.iris.gossip.serialize

import java.io.{DataInputStream, DataOutputStream}
import scala.collection.mutable.ListBuffer
import com.github.abulychev.iris.serialize.Serializer
import com.github.abulychev.iris.gossip.node.{GossipSyn, GossipDigest}

/**
 * User: abulychev
 * Date: 9/26/14
 */
class GossipSynSerializer(implicit val digestSerializer: Serializer[GossipDigest])
  extends LimitedSerializer[GossipSyn] {

  def writeTo(syn: GossipSyn, out: DataOutputStream, left: Int) {
    var l = left

    syn.digests foreach { case digest =>
      val bytes = digestSerializer.toBinary(digest)

      if (bytes.length <= l) {
        l -= bytes.length
        out.write(bytes, 0, bytes.length)
      }
    }
  }

  def readFrom(in: DataInputStream): GossipSyn = {
    val digests = ListBuffer.empty[GossipDigest]

    while (in.available() > 0) {
      digests += digestSerializer.readFrom(in)
    }

    GossipSyn(digests.toList)
  }
}
