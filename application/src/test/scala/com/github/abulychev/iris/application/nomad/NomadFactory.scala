package com.github.abulychev.iris.application.nomad

import java.io.File
import scala.util.Random
import com.github.abulychev.iris.application.RandomUtilities._

/**
 * User: abulychev
 * Date: 12/10/14
 */
object NomadFactory {
  def nomad(root: File): Stream[State] = {
    val state = new State(root)
    state #:: nomad(state)
  }

  def nomad(state: State): Stream[State] = {
    val action = choose(state)
    val next = state.make(action)
    next #:: nomad(next)
  }

  def repeat(root: File, history: List[Action]): Stream[State] = {
    val h = history.reverse
    assert(h.head == Root)
    val state = new State(root)
    state #:: repeat(state, h.tail)
  }

  def repeat(state: State, actions: List[Action]): Stream[State] = actions match {
    case Nil => Stream.Empty
    case x :: xs =>
      val next = state.make(x)
      next #:: repeat(next, xs)
  }

  private def choose(state: State): Action = {
    val name = nextName

    Random.nextInt(4) match {
      case 0 => Random.shuffle(state.directories).headOption.map(ChangeDirectory).getOrElse(Idle)
      case 1 => if (state.canUp) Up else Idle
      case 2 => if (!state.directories.contains(name) && !state.files.contains(name)) MakeDirectory(name) else Idle
      case 3 => if (!state.directories.contains(name)) Write(name, nextByteArray) else Idle
      case 5 => Random.shuffle(state.directories).headOption.map(RemoveDirectory).getOrElse(Idle)
      case 6 => Random.shuffle(state.directories).headOption.map(Delete).getOrElse(Idle)
    }
  }
}
