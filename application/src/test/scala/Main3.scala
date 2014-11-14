import com.github.abulychev.iris.application.ApplicationBuilder
import java.io.File
import java.net.{InetSocketAddress, InetAddress}

/**
 * User: abulychev
 * Date: 3/12/14
 */
object Main3 extends App {
  ApplicationBuilder.build(
    "/home/abulychev/hellofs3",
    new File("/home/abulychev/git/iris/application/data3"),
    InetAddress.getByName("127.0.0.1"),
    10347,
    11347,
    12347,
    List(new InetSocketAddress("127.0.0.1", 10345), new InetSocketAddress("127.0.0.1", 10346))
  )

}
