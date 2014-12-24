package com.github.abulychev.iris.gossip.node

import java.net.InetSocketAddress

/**
 * User: abulychev
 * Date: 9/26/14
 */
case class GossipDigest(endpoint: InetSocketAddress, generation: Int, version: Int)

sealed trait GossipMessage
case class GossipSyn(digests: List[GossipDigest]) extends GossipMessage
case class GossipAck[K,V](endpointStates: Map[InetSocketAddress, EndpointState[K,V]], digests: List[GossipDigest]) extends GossipMessage
case class GossipAck2[K,V](endpointStates: Map[InetSocketAddress, EndpointState[K,V]]) extends GossipMessage