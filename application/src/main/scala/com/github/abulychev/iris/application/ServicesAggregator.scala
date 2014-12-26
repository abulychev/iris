package com.github.abulychev.iris.application

import akka.actor.{ActorRef, Actor, ActorLogging}
import com.github.abulychev.iris.{RegisterService, Service}

/**
 * User: abulychev
 * Date: 12/25/14
 */
class ServicesAggregator(handler: ActorRef, amount: Int) extends Actor with ActorLogging {
  import ServicesAggregator._


  def awaiting(n: Int, services: Map[Service, ActorRef]): Receive = {
    if (n == 0) {
      handler ! Collected(services)
      context.stop(self)
    }

    PartialFunction {
      case RegisterService(service, actor) =>
        context.become(awaiting(n-1, services + (service -> actor)))
    }
  }

  def receive = awaiting(amount, Map.empty)

}

object ServicesAggregator {
  case class Collected(services: Map[Service, ActorRef])
}
