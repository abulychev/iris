package com.github.abulychev.iris.distributed.names

import java.net.InetSocketAddress
import com.github.abulychev.iris.storage.local.names.NamesBlock
import com.github.abulychev.iris.dht.actor.Token

/**
 * User: abulychev
 * Date: 10/21/14
 */
case class UpdateList(updates: Map[(Token, Long), NamesBlock])
