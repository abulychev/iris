package thrift

import com.twitter.finagle.example.thriftscala.Hello
import com.twitter.finagle.Thrift

/**
 * User: abulychev
 * Date: 11/7/14
 */
object ThriftClient extends App {
  val client = Thrift.newIface[Hello.FutureIface]("localhost:8080")
  client.hi() onSuccess { response =>
    println("Received response: " + response)
  }

  Thread.sleep(1000)
}
