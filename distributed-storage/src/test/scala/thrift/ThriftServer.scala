package thrift

import com.twitter.finagle.Thrift
import com.twitter.util.{Await, Future}
import com.twitter.finagle.example.thriftscala.Hello

/**
 * User: abulychev
 * Date: 11/7/14
 */
object ThriftServer extends App {
  val server = Thrift.serveIface("localhost:8080", new Hello[Future] {
    def hi() = Future.value("hi")
  })
  Await.ready(server)

}
