package com.github.abulychev.iris.gossip.serializer

/**
 * User: abulychev
 * Date: 9/26/14
 */

import org.scalatest._
import java.net.InetSocketAddress
import com.github.abulychev.iris.serialize.EndpointSerializer

class EndpointSerializerSuite extends FunSuite with BeforeAndAfter {
  test("serialize/deserialize test") {
    val endpoint1 = new InetSocketAddress("127.0.0.1", 12345)
    val bytes = EndpointSerializer.toBinary(endpoint1)
    val endpoint2 = EndpointSerializer.fromBinary(bytes)
    assert(endpoint1 == endpoint2)
  }
}
