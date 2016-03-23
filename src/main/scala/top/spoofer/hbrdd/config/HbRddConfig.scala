package top.spoofer.hbrdd.config

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

class HbRddConfig(config: Configuration) extends Serializable {
  def getHbaseConfig = HBaseConfiguration.create(config)
}

object HbRddConfig {
  type configOption = (String, String)
  private[HbRddConfig] case class HbaseOption(name: String, value: String)

  def apply(config: Configuration): HbRddConfig = new HbRddConfig(config)

  def apply(configs: configOption*): HbRddConfig = {
    val hbConfig = HBaseConfiguration.create()

    for {
      option <- configs
      hbOption = HbaseOption(option._1, option._2)  //使用新的case class 只是为了表达更加清晰
    } hbConfig.set(hbOption.name, hbOption.value)

    this.apply(hbConfig)
  }

  def apply(configs: { def rootDir: String; def quorum: String}): HbRddConfig = {
    apply(
      "hbase.rootdir" -> configs.rootDir,
      "hbase.zookeeper.quorum" -> configs.quorum
    )
  }

  def apply(configs: Map[String, String]): HbRddConfig = {
    val hbConfig = HBaseConfiguration.create()

    configs.keys foreach { name =>
      hbConfig.set(name, configs(name))
    }

    this.apply(hbConfig)
  }

  def apply(configs: TraversableOnce[configOption]): HbRddConfig = {
    val hbConfig = HBaseConfiguration.create()

    configs foreach { option =>
      val hbOption = HbaseOption(option._1, option._2)
      hbConfig.set(hbOption.name, hbOption.value)
    }

    this.apply(hbConfig)
  }
}
