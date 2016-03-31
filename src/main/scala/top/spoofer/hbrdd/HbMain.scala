package top.spoofer.hbrdd

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.spark.{SparkContext, SparkConf}
import top.spoofer.hbrdd.config.HbRddConfig
import top.spoofer.hbrdd._

object HbMain {
  private val master = "Core1"
  private val port = "7077"
  private val appName = "hbase-rdd_spark"
  private val data = "hdfs://Master1:8020/test/spark/hbase/testhb"

  private def testRdd2Hbase() = {
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

    sc.stop()
  }

  private def testRdd2HbaseTs() = {
    implicit val hbConfig = HbRddConfig()

    val sparkConf = new SparkConf()
      .setMaster(s"spark://$master:$port").set("executor-memory", "2g")
      .setAppName(appName).setJars(List("/home/lele/coding/hbrdd/out/artifacts/hbrdd_jar/hbrdd.jar"))

    val sc = new SparkContext(sparkConf)
    val ret = sc.textFile(data).map({ line =>
      val Array(k, col1, col2, _) = line split "\t"
      val content = Map("cf1" -> Map("testqualifier" -> (123L, col1)))
      k -> content  //(rowID, Map[family, Map[qualifier, value]])
    }).put2Hbase("test_hbrdd")

    sc.stop()
  }

  private def testSingleFamilyRdd2Hbase() = {
    implicit val hbConfig = HbRddConfig()

    val sparkConf = new SparkConf()
      .setMaster(s"spark://$master:$port").set("executor-memory", "2g")
      .setAppName(appName).setJars(List("/home/lele/coding/hbrdd/out/artifacts/hbrdd_jar/hbrdd.jar"))

    val sc = new SparkContext(sparkConf)
    val ret = sc.textFile(data).map({ line =>
      val Array(k, col1, col2, _) = line split "\t"
      val content = Map("testqualifier" -> col1)
      k -> content  //(rowID, Map[family, Map[qualifier, value]])
    }).put2Hbase("test_hbrdd", "cf1")

    sc.stop()
  }

  private def testSingleFamilyRdd2HbaseTs() = {
    implicit val hbConfig = HbRddConfig()

    val sparkConf = new SparkConf()
      .setMaster(s"spark://$master:$port").set("executor-memory", "2g")
      .setAppName(appName).setJars(List("/home/lele/coding/hbrdd/out/artifacts/hbrdd_jar/hbrdd.jar"))

    val sc = new SparkContext(sparkConf)
    val ret = sc.textFile(data).map({ line =>
      val Array(k, col1, col2, _) = line split "\t"
      val content = Map("testqualifier" -> (123L, col1))
      k -> content  //(rowID, Map[family, Map[qualifier, value]])
    }).put2Hbase("test_hbrdd", "cf1")

    sc.stop()
  }

  private def testReadHbase() = {
    import org.apache.hadoop.hbase.filter.{FilterList, SingleColumnValueFilter}
    implicit val hbConfig = HbRddConfig()
    val savePath = "hdfs://Master1:8020/test/spark/hbase/calculation_result"

    val sparkConf = new SparkConf()
      .setMaster(s"spark://$master:$port").set("executor-memory", "2g")
      .setAppName(appName).setJars(List("/home/lele/coding/hbrdd/out/artifacts/hbrdd_jar/hbrdd.jar"))
    val sc = new SparkContext(sparkConf)

    val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL)
    val filter1 = new SingleColumnValueFilter("cf1".getBytes, "testqualifier".getBytes, CompareOp.EQUAL, "'S0My9O0dN".getBytes)

    filterList.addFilter(filter1)
    val qualifiers = Set("testqualifier")
    val tableStructure = Map[String, Set[String]]("cf1"-> qualifiers)
    val families = Set[String]("cf1")
    val rdd = sc.readHbase[String]("test_hbrdd", families, filterList)

    println(rdd.count())
    rdd.saveAsTextFile(savePath)

    sc.stop()
  }

  private def testdeleteHbase() = {
    implicit val hbConfig = HbRddConfig()
    val savePath = "hdfs://Master1:8020/test/spark/hbase/calculation_result"

    val sparkConf = new SparkConf()
      .setMaster(s"spark://$master:$port").set("executor-memory", "2g")
      .setAppName(appName).setJars(List("/home/lele/coding/hbrdd/out/artifacts/hbrdd_jar/hbrdd.jar"))
    val sc = new SparkContext(sparkConf)

    val ret = sc.parallelize(List("!0?f^O|gsb", "!1f0\\I=@m*"), 3).deleteHbase("test_hbrdd")
    println(ret.getClass)
    sc.stop()
  }

  def main(args: Array[String]) {
    System.setProperty("HADOOP_USER_NAME", "hadoop")
//    this.testReadHbase()
//    this.testSingleFamilyRdd2Hbase()
    this.testdeleteHbase()
  }
}
