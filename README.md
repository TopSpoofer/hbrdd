# hbrdd

hbrdd是一个方便spark批量导入数据到hbase的库。开发hbrdd-1.0的时候依赖于cdh-5.5.1的集群环境,
spark依赖库为1.5.1-hadoop-2.6.0, hadoop 2.6.0, hbase 1.0.0。


由于hbrdd只是依赖于hbase、spark的通用库, 所以如果你的集群环境不是和上述的一样,
你可以下载源码修改build.sbt文件里hbase、spark的依赖库的版本, 使其与你的集群组件对应。


#### 安装hbrdd

因为考虑到不同的集群，组件的版本都可能不同，所以hbrdd没有打包放到mvn上。所以只能下载源码进行手动编译！

```
$ git clone git@github.com:TopSpoofer/hbrdd.git
$ cd hbrdd
//如果需要，你可以修改build.sbt文件里集群组件的版本来适应你的集群。
$ sbt
>> compile
>> package
$ 将打包好的jar放到你的项目的根目录的lib目录下。然后导入hbrdd的jar。
```

除了需要编译，导入jar，你还需要在你的项目的bulid.sbt下加入hbrdd的依赖包。hbrdd的依赖包如下：

```
val hbaseVersion = "1.1.1"
val hadoopVersion = "2.6.0"
val lang3Version = "3.0"
val jacksonVersion = "3.2.11"

val hbaseCommon = "org.apache.hbase" % "hbase-common" % hbaseVersion % "compile"
val hbaseClient = "org.apache.hbase" % "hbase-client" % hbaseVersion % "compile"
val hbaseServer = "org.apache.hbase" % "hbase-server" % hbaseVersion % "compile" excludeAll ExclusionRule(organization = "org.mortbay.jetty")

val lang3 = "org.apache.commons" % "commons-lang3" % lang3Version
val jackson = "org.json4s" %% "json4s-jackson" % jacksonVersion % "provided"

libraryDependencies ++= Seq(
  hbaseCommon,
  hbaseClient,
  hbaseServer,
  lang3,
  jackson
)
```

最后你还需要将你的spark的依赖库 spark-assembly-x.x.x-hadoopx.x.x.jar导入。


### 使用例子

#### 导入hbrdd

```
import top.spoofer.hbrdd._
import top.spoofer.hbrdd.config.HbRddConfig
```

hbrdd._ 提供了hbrdd的各个工具类，而config.HbRddConfig提供了解析配置文件的api。

#### 配置hbase连接参数

对于配置项，最基本的是 hbase.rootdir、hbase.zookeeper.quorum这两个配置项。

你可以编写一个hbase-site.xml的配置文件， 然后放入到src/main/resources/目录里（intellij idea sbt 项目结构）。
hbase-site.xml内容和格式，例如：

```
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
	<property>
		<name>hbase.rootdir</name>
		<value>/home/hbase</value>
	</property>
	<property>
		<name>hbase.zookeeper.quorum</name>
    <value>Core1,Core2,Core3</value>
  </property>
  <property>
    <name>hbase.zookeeper.property.dataDir</name>
    <value>/home/zookeeper</value>
  </property>
</configuration>
```

或者可以动态设定，方法如下：

```
private def testMapConfig() = {
    val c: Map[String, String] = Map("hbase.rootdir" -> "svn")

    implicit val config = HbRddConfig(c)

    //you code
    // ......

    val cfg = config.getHbaseConfig
    println(cfg.get("hbase.rootdir"))
}
```

完整的动态设定、创建HbRddConfig的例子请参见： [HbRddConfig-examples](https://github.com/TopSpoofer/hbrdd/blob/master/src/test/scala/TestHbConfig.scala)

#### hbase 表管理

hbrdd对hbase的表管理提供了如下api：

```
/**
  * 判断一个表和其列簇是否存在
  * @param tableName 表名字
  * @param family 列簇
  * @return 存在返回true否则false
  */
def tableExists(tableName: String, family: String): Boolean = {}
def tableExists(tableName: String, families: TraversableOnce[String]): Boolean = {}
def tableExists(tableName: String, families: String*): Boolean = {}
def tableExists(tableName: String): Boolean = {}


/**
  * 创建表
  * @param tableName 表名字
  * @param families 列簇
  * @param splitKeys 定义region splits的keys
  * @return
  */
def createTable(tableName: String, families: TraversableOnce[String], splitKeys: TraversableOnce[String]): HbRddAdmin = {}
def createTable(tableName:String, family: String, splitKeys: TraversableOnce[String]): HbRddAdmin = {}
def createTable(tableName: String, families: TraversableOnce[String]): HbRddAdmin = {}
def createTable(tableName: String, families: String*): HbRddAdmin = {}
def createTable(tableName: String, splitKeys: TraversableOnce[String], families: String*): HbRddAdmin = {}

/**
  * 使数据表变为可用
  * @param tableName 表名字
  * @return
  */
def enableTable(tableName: String): HbRddAdmin = {}

/**
  * 禁用一个数据表
  * @param tableName 数据表的名字
  * @param requireExists 如果为true, 当表不存在的时候会抛出异常
  * @return
  */
def disableTable(tableName: String, requireExists: Boolean = false): HbRddAdmin = {}

/**
  * 删除数据表, 在进行删除前需要disabletable, 否则会抛出异常
  * 这是一个通用的函数， 如果要直接删除表, 使用dropTable
  * @param tableName 表名字
  * @return
  */
def deleteTable(tableName: String): HbRddAdmin = {}

/**
  * 先 disable 表, 再delete table
  * @param tableName 表名字
  * @return
  */
def dropTable(tableName: String): HbRddAdmin = {}

/**
  * 先禁止table再截断
  * @param tableName 表名字
  * @param preserveSplits 是否保存分裂
  * @return
  */
def truncateTable(tableName: String, preserveSplits: Boolean): HbRddAdmin = {}

/**
  * 产生一个table快照
  * @param tableName 表名字
  * @param snapshotName 快照名字
  * @return
  */
def tableSnapshot(tableName: String, snapshotName: String): HbRddAdmin = {}

/**
  * 产生一个table快照, 使用默认的快照名字${tableName}_${yyyy-MM-dd-HHmmss}
  * @param tableName 表名字
  * @return
  */
def tableSnapshot(tableName: String): HbRddAdmin = {}

/**
  * 关闭admin的连接，使用完admin后必须关闭连接！
  */
def close() = {}
```

现在还不支持创建表的时候定义表的属性，例如ttl、max version等，日后会添加！

admin的使用请参见: [HbAdmin-examples](https://github.com/TopSpoofer/hbrdd/blob/master/src/test/scala/TestHbAdmin.scala)


#### 从hbase中读取数据

读取hbase数据的模块提供了较多的api，不过没有提供读取数据成为json的功能。

```
/**
  * 读取数据表中指定families和qualifier的数据
  * @param table 表名字
  * @param tableStructure 指定要获取的families和qualifier, Map(family, Set(qualifier))
  * @param config 配置
  * @param reader 格式化类
  * @tparam A 类型参数
  * @return (rowId, Map(family, Map(qualifier, value))))
  */
def readHbase[A](table: String, tableStructure: Map[String, Set[String]])(implicit config: HbRddConfig, reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, A]])] = {}

def readHbase[A](table: String, tableStructure: Map[String, Set[String]], filter: Filter)(implicit config: HbRddConfig, reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, A]])] = {}

def readHbase[A](table: String, tableStructure: Map[String, Set[String]], filters: FilterList)(implicit config: HbRddConfig, reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, A]])] = {}

def readHbase[A](table: String, tableStructure: Map[String, Set[String]], scanner: Scan)(implicit config: HbRddConfig, reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, A]])] = {}




/**
  * 读取hbase数据表指定family, qualifier的内容, 其中连ts也提取出来了
  * @param table 数据表
  * @param tableStructure 指定的families, qualifier, Map(family, Set(qualifier))
  * @param config 配置
  * @param reader 格式化读取函数
  * @tparam A 类型参数
  * @return (rowId, Map(family, Map(qualifier, (ts, value)))))
  */
def readHbaseTs[A](table: String, tableStructure: Map[String, Set[String]])(implicit config: HbRddConfig, reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, (Long, A)]])] = {}

def readHbaseTs[A](table: String, tableStructure: Map[String, Set[String]], filter: Filter)(implicit config: HbRddConfig, reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, (Long, A)]])] = {}

def readHbaseTs[A](table: String, tableStructure: Map[String, Set[String]], filters: FilterList)(implicit config: HbRddConfig, reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, (Long, A)]])] = {}

def readHbaseTs[A](table: String, tableStructure: Map[String, Set[String]], scanner: Scan)(implicit config: HbRddConfig, reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, (Long, A)]])] = {}




/**
  * 读取hbase数据表指定family的所有qualifier数据
  * @param table 数据表
  * @param tableStructure 指定的family
  * @param config 配置
  * @param reader 读取数据的格式化函数
  * @tparam A 类型参数
  * @return (rowID, Map(family, Map(qualifier, value)))
  */
def readHbase[A](table: String, tableStructure: Set[String])(implicit config: HbRddConfig, reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, A]])] = {}

def readHbase[A](table: String, tableStructure: Set[String], filter: Filter)(implicit config: HbRddConfig, reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, A]])] = {}

def readHbase[A](table: String, tableStructure: Set[String], filterList: FilterList)(implicit config: HbRddConfig, reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, A]])] = {}

def readHbase[A](table: String, tableStructure: Set[String], scanner: Scan)(implicit config: HbRddConfig, reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, A]])] = {}




/**
  * 读取hbase数据表指定family的所有qualifier数据, 其中连ts也提取了
  * @param table 数据表
  * @param tableStructure 指定的family
  * @param config 配置
  * @param reader 读取数据的格式化函数
  * @tparam A 类型参数
  * @return (rowId, Map(family, Map[qualifier, (ts, value)))) 类型的RDD
  */
def readHbaseTs[A](table: String, tableStructure: Set[String])(implicit config: HbRddConfig, reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, (Long, A)]])] = {}

def readHbaseTs[A](table: String, tableStructure: Set[String], filter: Filter)(implicit config: HbRddConfig, reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, (Long, A)]])] = {}

def readHbaseTs[A](table: String, tableStructure: Set[String], filterList: FilterList)(implicit config: HbRddConfig, reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, (Long, A)]])] = {}

def readHbaseTs[A](table: String, tableStructure: Set[String], scanner: Scan)(implicit config: HbRddConfig, reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, (Long, A)]])] = {}




/**
  * 启动作业进行读取表的内容
  * @param table 数据表
  * @param scanner 扫描器
  * @param config 配置
  * @tparam A 类型参数
  * @return 返回没有进行处理过的(rowId, Result) 类型的RDD
  */
def readHbase(table: String, filter: Filter)(implicit config: HbRddConfig): RDD[(String, Result)] = {}
def readHbase(table: String, filterList: FilterList)(implicit config: HbRddConfig): RDD[(String, Result)] = {}

```

使用示例：

```
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
```

详细请参看： [read-examples](https://github.com/TopSpoofer/hbrdd/blob/master/src/main/scala/top/spoofer/hbrdd/HbMain.scala)


#### 写入数据到hbase

hbrdd的写入api也有多个：

```
/**
  * (rowID, Map[family, Map[qualifier, value]])
  * @param rdd rdd
  * @param writer 格式化写入
  * @tparam A 类型参数
  * @return
  */
implicit def rdd2Hbase[A](rdd: RDD[(String, Map[String, Map[String, A]])])(implicit writer: HbRddFormatsWriter[A]): RDDWriter[A] = {}

/**
  * (rowID, Map[family, Map[qualifier, (ts, value)]])
  * @param rdd rdd
  * @param writer 格式化写入
  * @tparam A 类型参数
  * @return
  */
implicit def rdd2HbaseUseTs[A](rdd: RDD[(String, Map[String, Map[String, TsValue[A]]])])(implicit writer: HbRddFormatsWriter[A]): RDDWriter[TsValue[A]] = {}

implicit def singleFamilyRdd2Hbase[A](rdd: RDD[(String, Map[String, A])])(implicit writer: HbRddFormatsWriter[A]): SingleFamilyRDDWriter[A] = {}

implicit def singleFamilyRdd2HbaseTs[A](rdd: RDD[(String, Map[String, TsValue[A]])])(implicit writer: HbRddFormatsWriter[A]): SingleFamilyRDDWriter[TsValue[A]] = {}
```

上述的api都使用了隐式转换，所以使用的时候只需产生正确类型的rdd， 然后调用put2Hbase()函数。使用例子如下：

```
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
```

需要注意的是，当前版本的hbrdd写入模块，不支持如：

```
val content = Map("cf1" -> Map("testqualifier" -> col1), "cf2" -> Map("testqualifier2" -> 123))
k -> content  //(rowID, Map[family, Map[qualifier, value]])
```
这样的map类型： Map[String, Map[String, Any]。

其中data文件的格式是有四列，列间使用'\t'进行分隔。

更多的例子请参见： [write-examples](https://github.com/TopSpoofer/hbrdd/blob/master/src/main/scala/top/spoofer/hbrdd/HbMain.scala)


#### 删除表中数据

hbrdd支持删除表中的数据， 也提供多个api。

```
/**
  * @param rdd (rowId, Map(family, Set(qualifier or (ts, qualifier))))
  * @return
  */
implicit def rddDelete(rdd: RDD[(String, Map[String, Set[String]])]): RDDDelete[String] = {}

implicit def tsRddDelete(rdd: RDD[(String, Map[String, Set[(Long, String)]])]): RDDDelete[(Long, String)] = {}

implicit def singleFamilyRDDDelete(rdd: RDD[(String, Set[String])]): SingleFamilyRDDDelete[String] = {}


/**
  * @param rdd rdd(rowId, Set((ts, qualifier)))
  * @return
  */
implicit def tsSingleFamilyRDDDelete(rdd: RDD[(String, Set[(Long, String)])]): SingleFamilyRDDDelete[(Long, String)] = {}

implicit def rowKeyRDDDelete(rdd: RDD[String]): RowKeyRDDDelete = {}
```

同样是使用了隐式转换，使用的时候产生正确的rdd，调用deleteHbase()函数即可。使用例子如下：

```
private def testdeleteHbase() = {
  implicit val hbConfig = HbRddConfig()

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
```

这个删除操作会删除所有的版本， 如果想只删除最新的版本， 需要使用带有ts的值类型的删除api，且把ts设置为Long.MaxValue。

详细的使用例子请参见： [delete-examples](https://github.com/TopSpoofer/hbrdd/blob/master/src/main/scala/top/spoofer/hbrdd/HbMain.scala)
