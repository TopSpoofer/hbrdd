import org.apache.spark.{SparkContext, SparkConf}

object TestMain {
  private val master = "Core1"
  private val port = "7077"
  private val appName = "hbase-rdd_spark"
  private val data = "hdfs://Master1:8020/test/spark/hbase/testhb"

  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
      .setMaster(s"spark://$master:$port")
      .setAppName(appName).setJars(List("/home/lele/coding/hbrdd/out/artifacts/hbrdd_jar/hbrdd.jar"))

    val sc = new SparkContext(sparkConf)
    val ret = sc.textFile(data).map({ line =>
        val Array(k, col1, col2, _) = line split "\t"
        val content = Map("col1" -> col1, "col2" -> col2)
        k -> content
      })

    println(ret.count())

    sc.stop()
  }
}
