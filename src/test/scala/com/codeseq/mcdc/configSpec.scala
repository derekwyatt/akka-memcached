package com.codeseq.mcdc
     
import com.typesafe.config.ConfigFactory
import org.scalatest.{WordSpec, Matchers}

class configSpec extends WordSpec with Matchers {
  import com.codeseq.mcdc.ServersConfigComponent._
  import scala.collection.JavaConverters._

  val refConf = ConfigFactory.load()
  val multiConf = ConfigFactory.parseString("""|servers = [ {
                                               |    host = "host1"
                                               |    port = 11211
                                               |    num-connections = 1
                                               |  }, {
                                               |    host = "host2"
                                               |    port = 11212
                                               |    num-connections = 2
                                               |  }, {
                                               |    host = "host3"
                                               |    port = 11213
                                               |    num-connections = 3
                                               |  }
                                               |]""".stripMargin)

  "config" should { //{1
    "load the server config from the reference" in { //{2
      val servers = ServersConfigComponent.config(refConf.getConfigList("com.codeseq.mcdc.servers").asScala)
      servers should have size (1)
      val ServerInfo(host, port, numConnections) = servers.head
      host should be ("localhost")
      port should be (11211)
      numConnections should be (10)
    } //}2
    "load the server config with multiple hosts" in { //{2
      val servers = ServersConfigComponent.config(multiConf.getConfigList("servers").asScala)
      servers should have size (3)
      val a = servers(0)
      a.host should be ("host1")
      a.port should be (11211)
      a.numConnections should be (1)
      val b = servers(1)
      b.host should be ("host2")
      b.port should be (11212)
      b.numConnections should be (2)
      val c = servers(2)
      c.host should be ("host3")
      c.port should be (11213)
      c.numConnections should be (3)
    } //}2
  } //}1
}
// vim:fdl=1:
