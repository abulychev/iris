package com.github.abulychev.iris.filesystem

/**
 * User: abulychev
 * Date: 2/10/15
 */
trait Error extends Exception

object Error {
  object NoSuchFileOrDirectory extends Error
  object ResourceTemporarilyUnavailable extends Error
  object FileExists extends Error
  object NoDataAvailable extends Error
}
