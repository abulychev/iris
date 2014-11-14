package com.github.abulychev.iris

import org.scalatest.FunSuite
import com.github.abulychev.iris.storage.local.chunk.actor.Slice

/**
 * User: abulychev
 * Date: 10/24/14
 */
class SliceSuite extends FunSuite {
  test("get") {
    val data = List[Byte](1, 2, 3, 4, 5).toArray
    val slice = new Slice(data)
    assert(slice.get.toList == List[Byte](1, 2, 3, 4, 5))
  }

  test("write") {
    val data = List[Byte](1, 2, 3, 4, 5).toArray
    val data2 = List[Byte](0, 0, 0).toArray
    val data3 = List[Byte](6, 6).toArray
    val slice = new Slice(data)
    slice.write(data2, 1)
    slice.write(data3, 4)
    assert(slice.get.toList == List[Byte](1, 0, 0, 0, 6, 6))
    assert(slice.get.length == slice.length)
  }

  test("truncate less than size") {
    val data = List[Byte](1, 2, 3, 4, 5).toArray
    val slice = new Slice(data)
    slice.truncate(2)
    assert(slice.get.toList == List[Byte](1, 2))
    assert(slice.get.length == slice.length)
  }

  test("truncate more than size") {
    val data = List[Byte](1, 2, 3, 4, 5).toArray
    val slice = new Slice(data)
    slice.truncate(6)
    assert(slice.get.toList == List[Byte](1, 2, 3, 4, 5, 0))
    assert(slice.get.length == slice.length)
  }
}