package com.codeseq.mcdc

import com.typesafe.config.Config

trait ConfigComponent {
  type Config

  def config: Config
}

object ServersConfigComponent {
  case class ServerInfo(host: String, port: Int, numConnections: Int)

  def config(servers: Seq[Config]): Seq[ServerInfo] =
    servers map { conf =>
      ServerInfo(conf.getString("host"), conf.getInt("port"), conf.getInt("num-connections"))
    }
}

trait ServersConfigComponent extends ConfigComponent {
  import ServersConfigComponent._

  type Config <: ServersConfig

  trait ServersConfig {
    val serverInfo: Seq[ServerInfo]
  }
}
