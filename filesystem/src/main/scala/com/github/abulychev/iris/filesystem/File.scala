package com.github.abulychev.iris.filesystem

/**
 * User: abulychev
 * Date: 2/9/15
 */
object File {
  /* Commands */
  trait Command

  case class Read(size: Long, offset: Long) extends Command
  case class Write(bytes: Array[Byte], offset: Long) extends Command
  case class Truncate(offset: Long) extends Command
  object Close extends Command


  /* Answers */
  trait Answer
  case class DataRead(bytes: Array[Byte]) extends Answer

}
