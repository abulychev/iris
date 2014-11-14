package com.github.abulychev.iris.dht.storage.columns

import java.net.InetSocketAddress
import com.github.abulychev.iris.dht.storage.Column
import com.github.abulychev.iris.serialize.EndpointSerializer

/**
 * User: abulychev
 * Date: 10/20/14
 */
class EndpointColumn(val endpoint: InetSocketAddress) extends Column(EndpointSerializer.toBinary(endpoint)) {

}

object EndpointColumn {
  def apply(endpoint: InetSocketAddress) = new EndpointColumn(endpoint)

  def unapply(column: Column): Option[InetSocketAddress] = {
    Some(EndpointSerializer.fromBinary(column.value))
  }
}
