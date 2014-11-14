package com.github.abulychev.iris.gossip.actor

import akka.actor.{ActorLogging, Actor}
import com.github.abulychev.iris.gossip.Gossip

/**
 * User: abulychev
 * Date: 7/15/14
 */
class GossipManager extends Actor with ActorLogging {
  def receive = {
    case bind @ Gossip.Bind(_, _, _, _) =>
      context.actorOf(bind.props)

    case msg => log.error(s"Unhandled packet {$msg}")
  }
}