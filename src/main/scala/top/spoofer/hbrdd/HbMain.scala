package top.spoofer.hbrdd

import org.apache.spark.{SparkContext, SparkConf}
import top.spoofer.hbrdd.config.HbRddConfig
import top.spoofer.hbrdd._

object HbMain {
  private val master = "Core1"
  private val port = "7077"
  private val appName = "hbase-rdd_spark"
  private val data = "hdfs://Master1:8020/test/spark/hbase/testhb"

  def main(args: Array[String]) {
    implicit val hbConfig = HbRddConfig()

    val sparkConf = new SparkConf()
      .setMaster(s"spark://$master:$port")
      .setAppName(appName).setJars(List("/home/lele/coding/hbrdd/out/artifacts/hbrdd_jar/hbrdd.jar"))

    val sc = new SparkContext(sparkConf)
    val ret = sc.textFile(data).map({ line =>
      val Array(k, col1, col2, _) = line split "\t"
      val content = Map("col1" -> Map(col1 -> "1"))
      k -> content
    }).put2Hbase("skdnvc")

//    println(ret.count())

    sc.stop()
  }
}
