package com.github.abulychev.iris.gossip.node

import com.github.abulychev.iris.gossip.node.HeartbeatState.Generation
import com.sun.istack.internal.Nullable

/**
 * User: abulychev
 * Date: 9/26/14
 */
trait Versioned[T] { this: T =>
  def version: Int

  def *(that: T with Versioned[T]): T with Versioned[T] =
    if (this.version > that.version) this else that
}

case class HeartbeatState(generation: Generation, version: Int) extends Versioned[HeartbeatState] {
  def *(that: HeartbeatState): HeartbeatState = {
    if (this.generation > that.generation) this
    else if (this.generation < that.generation) that
    else super.*(that)
  }
}

object HeartbeatState {
  type Generation = Int
}



case class VersionedValue[V](value: V, version: Int) extends Versioned[VersionedValue[V]]

case class EndpointState[K, V](@Nullable heartbeatState: HeartbeatState,
                               applicationState: Map[K, VersionedValue[V]])
  extends Versioned[EndpointState[K,V]] {

  def version: Int = {
    val hbVersion = Option(heartbeatState).map(_.version).getOrElse(0)

    val appStateVersions = (applicationState.values map { _.version }).toList
    val appStateMaxVersion = if (appStateVersions.length > 0) appStateVersions.max else 0

    Math.max(hbVersion, appStateMaxVersion)
  }

}

case class ApplicationState[K, V](key: K, value: V)
