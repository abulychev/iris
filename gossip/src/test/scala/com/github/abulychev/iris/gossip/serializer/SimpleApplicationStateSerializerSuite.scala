package com.github.abulychev.iris.gossip.serializer

/**
 * User: abulychev
 * Date: 9/26/14
 */

import org.scalatest._
import com.github.abulychev.iris.serialize.Serializer
import com.github.abulychev.iris.gossip.node.ApplicationState

class SimpleApplicationStateSerializerSuite extends FunSuite with BeforeAndAfter {
  var serializer: Serializer[ApplicationState[String, String]] = _

  before {
    serializer = new SimpleApplicationStateSerializer
  }

  test("serialize/deserialize test") {
    val state1 = ApplicationState[String, String]("hello", "world!")
    val bytes = serializer.toBinary(state1)
    val state2 = serializer.fromBinary(bytes)
    assert(state1 == state2)
  }
}
