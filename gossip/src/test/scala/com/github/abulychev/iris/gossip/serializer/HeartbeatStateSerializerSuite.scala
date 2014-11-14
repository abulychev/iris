package com.github.abulychev.iris.gossip.serializer

/**
 * User: abulychev
 * Date: 9/26/14
 */

import org.scalatest._
import com.github.abulychev.iris.serialize.Serializer
import com.github.abulychev.iris.gossip.node.HeartbeatState
import com.github.abulychev.iris.gossip.serialize.{HeartbeatStateSerializer, GenerationSerializer}

class HeartbeatStateSerializerSuite extends FunSuite with BeforeAndAfter {
  var serializer: Serializer[HeartbeatState] = _

  before {
    implicit val generationSerializer = new GenerationSerializer
    serializer = new HeartbeatStateSerializer
  }

  test("serialize/deserialize test") {
    val hbState1 = HeartbeatState(10, 1)
    val bytes = serializer.toBinary(hbState1)
    val hbState2 = serializer.fromBinary(bytes)
    assert(hbState1 == hbState2)
  }
}
