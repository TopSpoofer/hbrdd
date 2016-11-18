package top.spoofer.hbrdd.hbsupport

import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{ CellUtil, Cell }
import org.apache.hadoop.hbase.client.{ Result, Scan }
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.filter.{ FilterList, Filter }
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import top.spoofer.hbrdd.config.HbRddConfig
import org.apache.hadoop.hbase.mapreduce.{ TableInputFormat, IdentityTableMapper, TableMapReduceUtil }

import top.spoofer.hbrdd.unit.HbRddFormatsReader
import top.spoofer.hbrdd._

trait HbRddReader {
  implicit def toHBaseSC(sc: SparkContext): HbaseSparkContext = new HbaseSparkContext(sc)
}

final class HbaseSparkContext(@transient sc: SparkContext) extends Serializable {
  private def extractCellValue[A](cell: Cell)(implicit reader: HbRddFormatsReader[A]): A = {
    reader.formatsRead(CellUtil.cloneValue(cell))
  }

  /**
   * 提取cell 的值, 带有时间戳
   * @param cell cell
   * @param reader 格式化value的函数
   * @tparam A A类型参数
   * @return (ts, value)
   */
  private def extractCellTsValue[A](cell: Cell)(implicit reader: HbRddFormatsReader[A]): (Long, A) = {
    (cell.getTimestamp, reader.formatsRead(CellUtil.cloneValue(cell)))
  }

  private def createConfig(table: String, hbConfig: HbRddConfig,
    qualifiers: Option[String] = None, scanner: Scan = new Scan): Configuration = {
    val config = hbConfig.getHbaseConfig
    if (qualifiers.isDefined) config.set(TableInputFormat.SCAN_COLUMNS, qualifiers.get)

    val job = Job.getInstance(config)
    TableMapReduceUtil.initTableMapperJob(table, scanner, classOf[IdentityTableMapper], null, null, job)
    job.getConfiguration
  }

  private def createScanner(filter: Filter) = new Scan().setFilter(filter)
  private def createScanner(filters: FilterList) = new Scan().setFilter(filters)

  /**
   * @param result 读取hbase返回的结果
   * 这个提取函数会提取表中指定family的所有内容,所以会把每一行的指定family的所有内容都提取出来
   * @param data families
   * @param extractCell 用来提取cell内容的函数
   * @tparam A A类型参数
   * @tparam B B类型参数
   * @return Map(family, Map(qualifier, value))
   */
  private def extractRow[A, B](result: Result, data: Set[String], extractCell: Cell => B) = {
    result.listCells() groupBy { cell =>
      new String(CellUtil.cloneFamily(cell))
    } filterKeys data.contains map {
      /* 不能使用mapValue， 因为其返回的是一个非可串行化的MapLike */
      case (family, cells) =>
        val cellsInfo = cells map { cell =>
          val qualifier = new String(CellUtil.cloneQualifier(cell))
          qualifier -> extractCell(cell)
        }
        (family, cellsInfo toMap)
    }
  }

  /**
   *
   * @param result 读取hbase返回的结果
   * 本函数提取表中指定family、qualifier中的内容。
   * @param data Map[family, Set[qualifier]
   * @param extractCell 提取cell内容的函数
   * @tparam A A类型参数
   * @tparam B B类型参数
   * @return Map(family, Map(qualifier, value))
   */
  private def extract[A, B](result: Result, data: Map[String, Set[String]], extractCell: Cell => B) = {
    data map {
      case (family, qualifiers) =>
        val cellInfo = qualifiers flatMap { qualifier =>
          Option {
            result.getColumnLatestCell(family, qualifier) //返回最新的值
          } map { cell =>
            qualifier -> extractCell(cell)
          }
        } toMap

        family -> cellInfo
    }
  }

  /**
   * 将要获取的表的family, qualifier解析成字符串如： family1:qualifier1 family1:qualifier2 family2:qualifier1
   * @param data Map(family, Set(qualifier))
   * @return
   */
  private def mkTableStructure(data: Map[String, Set[String]]): Option[String] = {
    val tableStructure = (for {
      (family, qualifiers) <- data
      qualifier <- qualifiers
    } yield s"$family:$qualifier") mkString " "

    if (tableStructure == "") None else Some(tableStructure)
  }

  private def mkTableStructure(families: Set[String]): Option[String] = {
    val tableStructure = families mkString " "
    if (tableStructure == "") None else Some(tableStructure)
  }

  /**
   * 读取数据表中指定families和qualifier的数据
   * @param table 表名字
   * @param tableStructure 指定要获取的families和qualifier, Map(family, Set(qualifier))
   * @param config 配置
   * @param reader 格式化类
   * @tparam A 类型参数
   * @return
   */
  def readHbase[A](table: String, tableStructure: Map[String, Set[String]])(implicit config: HbRddConfig, reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, A]])] = {
    this.readHbase(table, tableStructure, new Scan())
  }

  def readHbase[A](table: String, tableStructure: Map[String, Set[String]], filter: Filter)(implicit config: HbRddConfig, reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, A]])] = {
    this.readHbase(table, tableStructure, this.createScanner(filter))
  }

  def readHbase[A](table: String, tableStructure: Map[String, Set[String]], filters: FilterList)(implicit config: HbRddConfig, reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, A]])] = {
    this.readHbase(table, tableStructure, this.createScanner(filters))
  }

  def readHbase[A](table: String, tableStructure: Map[String, Set[String]], scanner: Scan)(implicit config: HbRddConfig, reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, A]])] = {
    this.readHbaseRaw(table, tableStructure, scanner) map {
      case (rowId, row) => Bytes.toString(rowId.get) -> this.extract(row, tableStructure, this.extractCellValue[A])
    }
  }

  /**
   * 读取hbase数据表指定family, qualifier的内容, 其中连ts也提取出来了
   * @param table 数据表
   * @param tableStructure 指定的families, qualifier
   * @param config 配置
   * @param reader 格式化读取函数
   * @tparam A 类型参数
   * @return (rowId, Map(family, Map(qualifier, (ts, value)))))
   */
  def readHbaseTs[A](table: String, tableStructure: Map[String, Set[String]])(implicit config: HbRddConfig, reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, (Long, A)]])] = {
    this.readHbaseTs(table, tableStructure, new Scan())
  }

  def readHbaseTs[A](table: String, tableStructure: Map[String, Set[String]], filter: Filter)(implicit config: HbRddConfig, reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, (Long, A)]])] = {
    this.readHbaseTs(table, tableStructure, this.createScanner(filter))
  }

  def readHbaseTs[A](table: String, tableStructure: Map[String, Set[String]], filters: FilterList)(implicit config: HbRddConfig, reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, (Long, A)]])] = {
    this.readHbaseTs(table, tableStructure, this.createScanner(filters))
  }

  def readHbaseTs[A](table: String, tableStructure: Map[String, Set[String]], scanner: Scan)(implicit config: HbRddConfig, reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, (Long, A)]])] = {
    this.readHbaseRaw(table, tableStructure, scanner) map {
      case (rowId, row) => Bytes.toString(rowId.get) -> this.extract(row, tableStructure, this.extractCellTsValue[A])
    }
  }

  /**
   * 启动作业进行读取表的内容
   * @param table 数据表
   * @param tableStructure 指定的families, qualifier
   * @param scanner 扫描器
   * @param config 配置
   * @tparam A 类型参数
   * @return
   */
  protected def readHbaseRaw[A](table: String, tableStructure: Map[String, Set[String]], scanner: Scan)(implicit config: HbRddConfig) = {
    val hbConfiguration = this.createConfig(table, config, this.mkTableStructure(tableStructure), scanner = scanner)
    sc.newAPIHadoopRDD(hbConfiguration, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
  }

  /**
   * 读取hbase数据表指定family的所有qualifier数据
   * @param table 数据表
   * @param tableStructure 指定的family
   * @param config 配置
   * @param reader 读取数据的格式化函数
   * @tparam A 类型参数
   * @return
   */
  def readHbase[A](table: String, tableStructure: Set[String])(implicit config: HbRddConfig, reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, A]])] = {
    this.readHbase(table, tableStructure, new Scan())
  }

  def readHbase[A](table: String, tableStructure: Set[String], filter: Filter)(implicit config: HbRddConfig, reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, A]])] = {
    this.readHbase(table, tableStructure, this.createScanner(filter))
  }

  def readHbase[A](table: String, tableStructure: Set[String], filterList: FilterList)(implicit config: HbRddConfig, reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, A]])] = {
    this.readHbase(table, tableStructure, this.createScanner(filterList))
  }

  def readHbase[A](table: String, tableStructure: Set[String], scanner: Scan)(implicit config: HbRddConfig, reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, A]])] = {
    this.readHbaseRaw(table, tableStructure, scanner) map {
      case (rowId, row) =>
        Bytes.toString(rowId.get()) -> this.extractRow(row, tableStructure, this.extractCellValue[A])
    }
  }

  /**
   * 读取hbase数据表指定family的所有qualifier数据, 其中连ts也提取了
   * @param table 数据表
   * @param tableStructure 指定的family
   * @param config 配置
   * @param reader 读取数据的格式化函数
   * @tparam A 类型参数
   * @return (rowId, Map(family, Map[qualifier, (ts, value))))
   */
  def readHbaseTs[A](table: String, tableStructure: Set[String])(implicit config: HbRddConfig, reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, (Long, A)]])] = {
    this.readHbaseTs(table, tableStructure, new Scan())
  }

  def readHbaseTs[A](table: String, tableStructure: Set[String], filter: Filter)(implicit config: HbRddConfig, reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, (Long, A)]])] = {
    this.readHbaseTs(table, tableStructure, this.createScanner(filter))
  }

  def readHbaseTs[A](table: String, tableStructure: Set[String], filterList: FilterList)(implicit config: HbRddConfig, reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, (Long, A)]])] = {
    this.readHbaseTs(table, tableStructure, this.createScanner(filterList))
  }

  def readHbaseTs[A](table: String, tableStructure: Set[String], scanner: Scan)(implicit config: HbRddConfig, reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, (Long, A)]])] = {
    this.readHbaseRaw(table, tableStructure, scanner) map {
      case (rowId, row) =>
        Bytes.toString(rowId.get()) -> this.extractRow(row, tableStructure, this.extractCellTsValue[A])
    }
  }

  /**
   * 启动作业进行读取表的内容
   * @param table 数据表
   * @param families 指定要读取的families
   * @param scanner 扫描器
   * @param config 配置
   * @tparam A 类型参数
   * @return
   */
  protected def readHbaseRaw[A](table: String, families: Set[String], scanner: Scan)(implicit config: HbRddConfig) = {
    val hbConfiguration = this.createConfig(table, config, this.mkTableStructure(families), scanner = scanner)
    sc.newAPIHadoopRDD(hbConfiguration, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
  }

  def readHbase(table: String, filter: Filter)(implicit config: HbRddConfig): RDD[(String, Result)] = {
    this.readHbaseRaw(table, this.createScanner(filter))
  }

  def readHbase(table: String, filterList: FilterList)(implicit config: HbRddConfig): RDD[(String, Result)] = {
    this.readHbaseRaw(table, this.createScanner(filterList))
  }

  protected def readHbaseRaw(table: String, scanner: Scan = new Scan)(implicit config: HbRddConfig) = {
    val hbConfiguration = this.createConfig(table, config, scanner = scanner)
    sc.newAPIHadoopRDD(hbConfiguration, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result]) map {
      case (key, result) =>
        Bytes.toString(key.get()) -> result
    }
  }
}