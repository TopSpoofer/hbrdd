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
      val content = Map("cf1" -> Map("testqualifier" -> col1), "cf2" -> Map("testqualifier2" -> col2))
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

    val ret = sc.parallelize(List("!7ls>'L%yz", "!AIvygne\\9"), 3).deleteHbase("test_hbrdd")

    /* 删除cf2的testqualifier2的内容,  可以指定多个family的多个qualifier */
//    val ret = sc.parallelize(List("\"'K.\"B\"@bE", "\")l8I=_$@W"), 3).deleteHbase("test_hbrdd", Map("cf1" -> Set("testqualifier")))
    /* 删除cf2的testqualifier2的内容, 可以指定cf2中多个qualifier */
//    val ret = sc.parallelize(List("!t@B+b3`5?"), 3).deleteHbase("test_hbrdd", "cf2", Set("testqualifier2"))
    /* 删除cf1, 不管cf1中有多少个qualifier */
//    val ret = sc.parallelize(List("!]emBlu0Gd", "!c+LpGh-UN"), 3).deleteHbase("test_hbrdd", Set("cf1"))

    println(ret.getClass)
    sc.stop()
  }

  private def testdeleteHbase1() = {
    implicit val hbConfig = HbRddConfig()
    val savePath = "hdfs://Master1:8020/test/spark/hbase/calculation_result"

    val sparkConf = new SparkConf()
      .setMaster(s"spark://$master:$port").set("executor-memory", "2g")
      .setAppName(appName).setJars(List("/home/lele/coding/hbrdd/out/artifacts/hbrdd_jar/hbrdd.jar"))
    val sc = new SparkContext(sparkConf)

    val qualifier = Set("testqualifier")

//    val rdd = sc.parallelize(List("!Hv[|[-K*o", "!IOJ]'M%o5"), 3).map({ rowId =>
////      val content = Map("cf1" -> Set("testqualifier"))
//      rowId  -> Set("testqualifier")
//    })
//
//    rdd.deleteHbase("test_hbrdd", "cf1")
//    rdd.saveAsTextFile(savePath)

    val ret = sc.textFile(data).map({ line =>
      val Array(rowId, col1, col2, _) = line split "\t"
      val content = Map("cf1" -> Set("testqualifier"))
      rowId -> content  //(rowID, Map[family, Set[qualifier]])
    }).deleteHbase("test_hbrdd")

    println(ret.getClass)
    sc.stop()
  }

//  def main(args: Array[String]) {
//    System.setProperty("HADOOP_USER_NAME", "hadoop")
////    this.testRdd2Hbase()
////    this.testReadHbase()
////    this.testSingleFamilyRdd2Hbase()
////    this.testdeleteHbase()
////    this.testdeleteHbase1()
//  }
}
