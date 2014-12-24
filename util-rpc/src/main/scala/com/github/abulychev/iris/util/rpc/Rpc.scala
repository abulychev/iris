package com.github.abulychev.iris.util.rpc

import akka.util.ByteString
import scala.concurrent.duration.Duration

/**
 * User: abulychev
 * Date: 12/15/14
 */
object Rpc {
  class Request private(val bytes: ByteString, private[rpc] val timeout: Duration)
  object Request {
    def apply(bytes: ByteString, timeout: Duration): Request =
      new Request(bytes, timeout)

    def apply(bytes: ByteString): Request =
      new Request(bytes, Duration.Undefined)

    def apply(bytes: Array[Byte], timeout: Duration): Request =
      apply(ByteString(bytes), timeout)

    def unapply(request: Request) = Some(request.bytes)
  }

  case class Push(bytes: ByteString)
  object Push {
    def apply(bytes: Array[Byte]): Push =
      Push(ByteString(bytes))
  }


  case class Response(bytes: ByteString)
  object Response {
    def apply(bytes: Array[Byte]): Response =
      Response(ByteString(bytes))
  }

  case object Timeout
}
