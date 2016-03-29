package top.spoofer.hbrdd.hbsupport


import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{CellUtil, Cell}
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.filter.Filter
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import top.spoofer.hbrdd.config.HbRddConfig
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, IdentityTableMapper, TableMapReduceUtil}

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

  private def mkTableStructure(data: Map[String, Set[String]]): Option[String] = {
    val tableStructure =  (for {
      (family, qualifiers) <- data
      qualifier <- qualifiers
    } yield s"$family:$qualifier") mkString " "

    if (tableStructure == "") None else Some(tableStructure)
  }

  def readHbase[A](table: String, tableStructure: Map[String, Set[String]])
                  (implicit config: HbRddConfig,
                   reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, A]])] = {

    null
  }

  def readHbase[A](table: String, tableStructure: Map[String, Set[String]], scanner: Scan)
                  (implicit config: HbRddConfig,
                   reader: HbRddFormatsReader[A]): RDD[(String, Map[String, Map[String, A]])] = {

    this.readHbaseRaw(table, tableStructure, scanner) map {
      case (rowId, row) => Bytes.toString(rowId.get) -> this.extract(row, tableStructure, reader[A])

    }
  }

  private def readHbaseRaw[A](table: String, tableStructure: Map[String, Set[String]],
                           scanner: Scan)(implicit config: HbRddConfig) = {
    val hbConfiguration = this.createConfig(table, config, this.mkTableStructure(tableStructure), scanner = scanner)

    sc.newAPIHadoopRDD(hbConfiguration, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result])
  }

  def readHbaseRaw(table: String, scanner: Scan = new Scan)(implicit config: HbRddConfig) = {
    val hbConfiguration = this.createConfig(table, config, scanner = scanner)
    sc.newAPIHadoopRDD(hbConfiguration, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result]) map { case (key, result) =>
        Bytes.toString(key.get()) -> result
    }
  }
}