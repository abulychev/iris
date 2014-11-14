package com.github.abulychev.iris.gossip.serializer

/**
 * User: abulychev
 * Date: 9/26/14
 */

import org.scalatest._
import com.github.abulychev.iris.serialize.Serializer
import com.github.abulychev.iris.gossip.node.{HeartbeatState, VersionedValue, EndpointState}
import com.github.abulychev.iris.gossip.serialize.{EndpointStateSerializer, HeartbeatStateSerializer, GenerationSerializer}

class EndpointStateSerializerSuite extends FunSuite with BeforeAndAfter {
  var serializer: Serializer[EndpointState[String, String]] = _

  before {
    implicit val appStateSerializer = new SimpleApplicationStateSerializer
    implicit val generationSerializer = new GenerationSerializer
    implicit val hbStateSerializer = new HeartbeatStateSerializer
    serializer = new EndpointStateSerializer[String, String]
  }

  test("serialize/deserialize test") {
    val hbState = HeartbeatState(10, 1)
    val appState = Map[String, VersionedValue[String]](
      "hello" -> VersionedValue("world!", 1)
    )

    val state1 = EndpointState[String, String](hbState, appState)
    val bytes = serializer.toBinary(state1)
    val state2 = serializer.fromBinary(bytes)
    assert(state1 == state2)
  }
}
