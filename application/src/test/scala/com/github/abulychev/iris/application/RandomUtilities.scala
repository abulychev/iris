package com.github.abulychev.iris.application

import scala.util.Random

/**
 * User: abulychev
 * Date: 12/12/14
 */
object RandomUtilities {
  def nextName: String = nextName(Random.nextInt(6) + 5)

  def nextName(len: Int): String = len match {
    case 0 => ""
    case _ => nextName(len - 1) + ('a' + Random.nextInt(26)).toChar
  }

  def nextByteArray: Array[Byte] = nextByteArray(Random.nextInt(5000000))

  def nextByteArray(len: Int): Array[Byte] =  {
    val bytes = new Array[Byte](len)
    Random.nextBytes(bytes)
    bytes
  }
}
