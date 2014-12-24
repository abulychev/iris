package com.github.abulychev.iris.util.rpc.tcp

import akka.actor.{ActorRef, Actor}
import scala.collection.mutable
import com.github.abulychev.iris.util.rpc._
import akka.actor.Terminated
import java.net.InetSocketAddress

/**
 * User: abulychev
 * Date: 12/17/14
 */
class ClientManager extends Actor {
  val ep2actor = mutable.Map.empty[InetSocketAddress, ActorRef]
  val actor2ep = mutable.Map.empty[ActorRef, InetSocketAddress]

  def receive = {
    case WriteTo(endpoint, r) =>
      sessionOf(endpoint) forward r

    case ClientSession.Detach =>
      sender ! ClientSession.Acknowledged
      remove(sender())
      context.unwatch(sender())

    case Terminated(actor) =>
      remove(actor)
  }

  def sessionOf(endpoint: InetSocketAddress): ActorRef = {
    if (ep2actor.contains(endpoint)) ep2actor(endpoint)
    else {
      val session = context.actorOf(ClientSession.props(endpoint, self))
      ep2actor += endpoint -> session
      actor2ep += session -> endpoint
      context.watch(session)
      session
    }
  }

  def remove(actor: ActorRef) {
    actor2ep.get(actor) foreach { ep =>
      actor2ep -= actor
      ep2actor -= ep
    }
  }
}

