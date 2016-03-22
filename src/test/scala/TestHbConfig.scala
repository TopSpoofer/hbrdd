import top.spoofer.hbrdd.config.HbRddConfig

object TestHbConfig {
  private def testMapConfig() = {
    val c: Map[String, String] = Map("hbase.rootdir" -> "svn")

    implicit val config = HbRddConfig(c)

    val cfg = config.getHbaseConfig
    println(cfg.get("hbase.rootdir"))
  }

  private def testListConfig() = {
    val l = List(("hbase.rootdir", "svn"))

    implicit val config = HbRddConfig(l)

    val cfg = config.getHbaseConfig
    println(cfg.get("hbase.rootdir"))
  }

  private def testTuplesConfig() = {
    implicit val config = HbRddConfig("hbase.rootdir" -> "svn", "hbase.zookeeper.quorum" -> "122")

    val cfg = config.getHbaseConfig
    println(cfg.get("hbase.rootdir"))
  }

  private def testClassConfig() = {
    case class Config(rootDir: String, quorum: String)

    val c = Config("1123", "27384")
    implicit val config = HbRddConfig(c)

    val cfg = config.getHbaseConfig
    println(cfg.get("hbase.rootdir"))
  }

  private def testConfig() = {
    implicit val config = HbRddConfig()
    val cfg = config.getHbaseConfig
    println(cfg.get("hbase.rootdir"))
  }

  def main(args: Array[String]) {
    this.testConfig()
  }
}
