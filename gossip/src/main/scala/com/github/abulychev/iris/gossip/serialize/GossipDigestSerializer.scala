package com.github.abulychev.iris.gossip.serialize

import java.io.{DataInputStream, DataOutputStream}
import java.net.InetSocketAddress
import com.github.abulychev.iris.serialize.Serializer
import com.github.abulychev.iris.gossip.node.HeartbeatState.Generation
import com.github.abulychev.iris.gossip.node.GossipDigest

/**
 * User: abulychev
 * Date: 9/26/14
 */
class GossipDigestSerializer(implicit val endpointSerializer: Serializer[InetSocketAddress],
                             implicit val generationSerializer: Serializer[Generation])
  extends Serializer[GossipDigest] {

  def writeTo(digest: GossipDigest, out: DataOutputStream) {
    endpointSerializer.writeTo(digest.endpoint, out)
    generationSerializer.writeTo(digest.generation, out)
    out.writeInt(digest.version)
  }

  def readFrom(in: DataInputStream): GossipDigest = {
    val endpoint = endpointSerializer.readFrom(in)
    val generation = generationSerializer.readFrom(in)
    val version = in.readInt()
    GossipDigest(endpoint, generation, version)
  }

}
