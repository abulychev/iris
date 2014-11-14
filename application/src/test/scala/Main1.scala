import com.github.abulychev.iris.application.ApplicationBuilder
import java.io.File
import java.net.{InetSocketAddress, InetAddress}

/**
 * User: abulychev
 * Date: 3/12/14
 */
object Main1 extends App {
  ApplicationBuilder.build(
    "/home/abulychev/hellofs",
    new File("/home/abulychev/git/iris/application/data1"),
    InetAddress.getByName("127.0.0.1"),
    10345,
    11345,
    12345,
    List(new InetSocketAddress("127.0.0.1", 10345), new InetSocketAddress("127.0.0.1", 10346))
  )
}
