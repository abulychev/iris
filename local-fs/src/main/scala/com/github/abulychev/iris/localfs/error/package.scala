package com.github.abulychev.iris.localfs

import net.fusejna.ErrorCodes

/**
 * User: abulychev
 * Date: 3/25/14
 */
package object error {
  abstract class ErrorCode(val code: Int) extends Exception

  object NoSuchFileOrDirectory extends ErrorCode(ErrorCodes.ENOENT)
  object ResourceTemporarilyUnavailable extends ErrorCode(ErrorCodes.EAGAIN)
  object FileExists extends ErrorCode(ErrorCodes.EEXIST)
  object NoDataAvailable extends ErrorCode(ErrorCodes.ENODATA)
}
