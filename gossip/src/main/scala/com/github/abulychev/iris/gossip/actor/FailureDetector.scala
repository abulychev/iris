package com.github.abulychev.iris.gossip.actor

import akka.actor.{Props, ActorLogging, ActorRef, Actor}
import scala.collection.mutable
import java.net.InetSocketAddress
import com.github.abulychev.iris.gossip.node.ArrivalWindow
import com.github.abulychev.iris.gossip.node.HeartbeatState.Generation
import com.github.abulychev.iris.settings.IrisSettings

/**
 * User: abulychev
 * Date: 10/3/14
 */
class FailureDetector(handler: ActorRef) extends Actor with ActorLogging with IrisSettings {
  import FailureDetector._

  private val windows = mutable.Map.empty[InetSocketAddress, ArrivalWindow]
  private val quarantine = mutable.Map.empty[InetSocketAddress, Long]
  private val bootstrapping = mutable.Set.empty[InetSocketAddress]
  private val generations = mutable.Map.empty[InetSocketAddress, Generation]

  private val phiThreshold = settings.Gossip.PhiThreshold
  private val failureDetectingInterval = settings.Gossip.FailureDetectingInterval

  override def preStart() {
    import scala.concurrent.ExecutionContext.Implicits.global
    context.system.scheduler.schedule(failureDetectingInterval, failureDetectingInterval, self, CheckEndpointStates)
  }

  def receive = {
    case Discovered(endpoint, generation) =>
      val tNow = System.currentTimeMillis

      val window = new ArrivalWindow
      window.add(tNow)

      windows += endpoint -> window
      quarantine += endpoint -> (tNow + ArrivalWindow.DefaultArrivalInterval)
      bootstrapping += endpoint
      generations += endpoint -> generation

    case Restarted(endpoint, generation) =>
      generations += endpoint -> generation

    case Heartbeat(endpoint) =>
      require(windows.contains(endpoint))
      val tNow = System.currentTimeMillis
      windows(endpoint).add(tNow)
      bootstrapping -= endpoint


    case CheckEndpointStates =>
      windows.keys foreach { case endpoint =>
        if (!bootstrapping.contains(endpoint)) {
          val window = windows(endpoint)
          val tNow = System.currentTimeMillis
          val phi = window.phi(tNow)

          log.info(s"$endpoint: $phi")

          if (phi > phiThreshold) {
            val quarantined = quarantine.contains(endpoint)
            quarantine += endpoint -> (tNow + ArrivalWindow.MaxInterval)
            if (!quarantined) {
              handler ! Unreachable(endpoint, generations(endpoint))
            }
          } else {
            if (quarantine.contains(endpoint) && quarantine(endpoint) < tNow) {
              quarantine -= endpoint
              handler ! Reachable(endpoint, generations(endpoint))
            }
          }
        }
      }
  }
}

object FailureDetector {
  def props(handler: ActorRef) = Props(classOf[FailureDetector], handler) 
  
  case class Discovered(endpoint: InetSocketAddress, generation: Generation)
  case class Restarted(endpoint: InetSocketAddress, generation: Generation)
  case class Heartbeat(endpoint: InetSocketAddress)

  case class Reachable(endpoint: InetSocketAddress, generation: Generation)
  case class Unreachable(endpoint: InetSocketAddress, generation: Generation)

  private case object CheckEndpointStates
}
