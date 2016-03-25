package top.spoofer.hbrdd

import org.apache.spark.{SparkContext, SparkConf}
import top.spoofer.hbrdd.config.HbRddConfig

object HbMain {
  private val master = "Core1"
  private val port = "7077"
  private val appName = "hbase-rdd_spark"
  private val data = "hdfs://Master1:8020/test/spark/hbase/testhb"

  def main(args: Array[String]) {
    implicit val hbConfig = HbRddConfig()

    val sparkConf = new SparkConf()
      .setMaster(s"spark://$master:$port").set("executor-memory", "2g")
      .setAppName(appName).setJars(List("/home/lele/coding/hbrdd/out/artifacts/hbrdd_jar/hbrdd.jar"))

//    val sparkConf = new SparkConf().setAppName(appName)

    val sc = new SparkContext(sparkConf)
    val ret = sc.textFile(data).map({ line =>
      val Array(k, col1, col2, _) = line split "\t"
      val content = Map("cf1" -> Map("testqualifier" -> col1))
      k -> content  //(rowID, Map[family, Map[qualifier, value]])
    }).put2Hbase("test_hbrdd")

//    println(ret.count())

    sc.stop()
  }
}
