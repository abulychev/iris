package com.github.abulychev.iris.application.nomad

import java.io.File
import org.apache.commons.io.FileUtils

/**
 * User: abulychev
 * Date: 12/10/14
 */
class State(root: File, current: File, up: List[File], val history: List[Action]) {
  def this(root: File) = this(root, root, Nil, List(Root))

  private val sub = current.listFiles.toList

  val files = sub.filter(_.isFile).map(_.getName)
  val directories = sub.filter(_.isDirectory).map(_.getName)
  val canUp = up.nonEmpty

  def make(action: Action): State = {
    def refresh: State = new State(root, current, up, action :: history)

    action match {
      case Idle => refresh

      case Root => new State(root)

      case ChangeDirectory(name) =>
        assert(directories.contains(name))
        new State(root, new File(current, name), current :: up, action :: history)

      case Up =>
        assert(canUp)
        new State(root, up.head, up.tail, action :: history)

      case MakeDirectory(name) =>
        assert(!directories.contains(name) && !files.contains(name))
        new File(current, name).mkdir()
        refresh

      case RemoveDirectory(name) =>
        assert(directories.contains(name))
        FileUtils.deleteDirectory(new File(current, name))
        refresh

      case Write(name, content) =>
        assert(!directories.contains(name))
        FileUtils.writeByteArrayToFile(new File(current, name), content)
        refresh

      case Delete(name) =>
        assert(directories.contains(name))
        new File(current, name).delete()
        refresh
    }
  }

}
