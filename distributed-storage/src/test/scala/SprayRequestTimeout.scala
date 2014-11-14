import akka.actor._
import akka.io.IO
import scala.Some
import spray.can.client.HostConnectorSettings
import spray.can.Http
import spray.can.Http.HostConnectorSetup
import spray.http.HttpMethods._
import spray.http._
import spray.http.HttpRequest
import scala.concurrent.duration._

/**
 * User: abulychev
 * Date: 10/31/14
 */
object SprayRequestTimeout extends App {
  val system = ActorSystem()
  system.actorOf(Props(classOf[TestActor]))

}

class TestActor extends Actor with ActorLogging {
  private val setup = HostConnectorSetup(
    host = "10.34.1.130",
    port = 80,
    settings = {
      val settings = HostConnectorSettings(context.system)
      val connections = settings.connectionSettings.copy(
        connectingTimeout = 1 seconds,
        requestTimeout = 5 seconds
      )
      Some(settings.copy(
        connectionSettings = connections,
        maxRetries = 0
      ))
    }
  )

  IO(Http)(context.system) ! (HttpRequest(GET, Uri("http://10.34.1.130")) -> setup)
  def receive = {
    case Status.Failure(_) =>
  }

}
