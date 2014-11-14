import com.github.abulychev.iris.application.ApplicationBuilder
import java.io.File
import java.net.{InetSocketAddress, InetAddress}

/**
 * User: abulychev
 * Date: 3/12/14
 */
object Main2 extends App {
  ApplicationBuilder.build(
    "/home/abulychev/hellofs2",
    new File("/home/abulychev/git/iris/application/data2"),
    InetAddress.getByName("127.0.0.1"),
    10346,
    11346,
    12346,
    List(new InetSocketAddress("127.0.0.1", 10345), new InetSocketAddress("127.0.0.1", 10346))
  )
}
