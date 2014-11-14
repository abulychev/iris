package com.github.abulychev.iris.gossip.node

import scala.collection.mutable
import java.net.InetSocketAddress
import scala.collection.mutable.ListBuffer
import com.github.abulychev.iris.gossip.node.HeartbeatState.Generation

/**
 * User: abulychev
 * Date: 9/26/14
 */
trait StatesHolder[K,V] {
  def localAddress: InetSocketAddress

  private val epStates = mutable.Map.empty[InetSocketAddress, EndpointState[K,V]]

  def generations(endpoint: InetSocketAddress) =
    epStates(endpoint).heartbeatState.generation

  def generateDigests: List[GossipDigest] = {
    val digest = epStates map { case (endpoint, epState) =>
      GossipDigest(endpoint, epState.heartbeatState.generation, epState.version)
    }
    digest.toList
  }

  private def mergeWith(onDiscovered: (InetSocketAddress, Generation) => Unit,
                onRestarted: (InetSocketAddress, Generation) => Unit,
                onHeartbeat: InetSocketAddress => Unit)
               (endpoint: InetSocketAddress, hbState: HeartbeatState) {

    if (epStates.contains(endpoint)) {
      val epState = epStates(endpoint)
      val oldHbState = epState.heartbeatState
      val newHbState = oldHbState * hbState

      if (newHbState.generation > oldHbState.generation) {
        /* RESTARTED */
        epStates += endpoint -> epState.copy(heartbeatState = newHbState, applicationState = Map.empty)
        onRestarted(endpoint, newHbState.generation)
      } else if (newHbState.version > oldHbState.version) {
        /* HEARTBEAT*/
        epStates += endpoint -> epState.copy(heartbeatState = newHbState)
        onHeartbeat(endpoint)
      }
    } else {
      /* NEW PEER */
      epStates += endpoint -> EndpointState(hbState, Map.empty)
      onDiscovered(endpoint, hbState.generation)
    }
  }

  private def mergeWith(onUpdated: (InetSocketAddress, ApplicationState[K,V]) => Unit)
               (endpoint: InetSocketAddress, appStates: Map[K, VersionedValue[V]]) {

    require(epStates.contains(endpoint))

    val epState = epStates(endpoint)
    val newStates = mutable.Map.empty[K, VersionedValue[V]]
    newStates ++= epState.applicationState

    val orderedStates = appStates.toList.sortBy { case (_, vv) => vv.version }

    orderedStates map { case (key, vv @ VersionedValue(value, version) ) =>
      if (!newStates.contains(key)) {
        /* NEW STATE */
        newStates += key -> vv
        onUpdated(endpoint, ApplicationState(key, value))
      } else {
        val oldVV = newStates(key)
        val newVV = oldVV * vv

        if (oldVV.version < vv.version) {
          /* STATE UPDATED */
          newStates += key -> newVV
          onUpdated(endpoint, ApplicationState(key, value))
        }
      }
    }

    epStates += endpoint -> epState.copy(applicationState = newStates.toMap)
  }

  private def mergeWithEndpointState(onDiscovered: (InetSocketAddress, Generation) => Unit,
                             onRestarted: (InetSocketAddress, Generation) => Unit,
                             onHeartbeat: InetSocketAddress => Unit,
                             onUpdated: (InetSocketAddress, ApplicationState[K,V]) => Unit)
                            (endpoint: InetSocketAddress, epState: EndpointState[K,V]) {

    if (epState.heartbeatState != null) {
      mergeWith(onDiscovered, onRestarted, onHeartbeat)(endpoint, epState.heartbeatState)
    }

    mergeWith(onUpdated)(endpoint, epState.applicationState)
  }

  def mergeWith(onDiscovered: (InetSocketAddress, Generation) => Unit,
                onRestarted: (InetSocketAddress, Generation) => Unit,
                onHeartbeat: InetSocketAddress => Unit,
                onUpdated: (InetSocketAddress, ApplicationState[K,V]) => Unit)
               (epStates: Map[InetSocketAddress, EndpointState[K,V]]) {

    epStates foreach { case (endpoint, epState) =>
      mergeWithEndpointState(onDiscovered, onRestarted, onHeartbeat, onUpdated)(endpoint, epState)
    }
  }

  def generateDigests(digests: List[GossipDigest]) = {
    val gDigests = ListBuffer.empty[GossipDigest]
    digests foreach { case digest @ GossipDigest(endpoint, generation, version) =>
      if (!epStates.contains(endpoint)) {
        gDigests += GossipDigest(endpoint, generation, 0)
      } else {
        val epState = epStates(endpoint)
        if (epState.heartbeatState.generation < generation) {
          gDigests += GossipDigest(endpoint, generation, 0)
        } else {
          if (epState.version < version) {
            gDigests += GossipDigest(endpoint, generation, epState.version)
          }
        }
      }
    }
    gDigests.toList
  }

  def generateEpStateMap(digests: List[GossipDigest]) = {
    val epStateMap = mutable.Map.empty[InetSocketAddress, EndpointState[K,V]]
    epStates foreach { case (endpoint, epState) =>
      val digest = digests.find(_.endpoint == endpoint)
      if (digest.isEmpty) {
        epStateMap += endpoint -> epState
      } else {
        val generation = digest.get.generation
        val version = digest.get.version

        if (epState.heartbeatState.generation > generation) {
          epStateMap += endpoint -> epState
        } else {
          if (epState.version > version) {
            val hbState = if (epState.heartbeatState.version > version) epState.heartbeatState else null
            val appStates = epState.applicationState.filter { case (_, VersionedValue(_, v)) => v > version }
            epStateMap += endpoint -> EndpointState(hbState, appStates)
          }
        }
      }
    }
    epStateMap.toMap
  }

  def incrementHeartbeat() {
    val oldEpState = epStates(localAddress)
    val oldHbState = oldEpState.heartbeatState

    val hbState = oldHbState.copy(version = oldEpState.version + 1)
    val epState = oldEpState.copy(heartbeatState = hbState)

    epStates += localAddress -> epState
  }

  def init(generation: Generation) {
    epStates += localAddress -> EndpointState(HeartbeatState(generation, 1), Map.empty)
  }

  def setState(state: ApplicationState[K,V]) {
    require(epStates.contains(localAddress))
    val epState = epStates(localAddress)

    val version = epState.version + 1

    val newAppState = epState.applicationState + (state.key -> VersionedValue(state.value, version))
    val newEpState = epState.copy(applicationState = newAppState)

    epStates += localAddress -> newEpState
  }
}
