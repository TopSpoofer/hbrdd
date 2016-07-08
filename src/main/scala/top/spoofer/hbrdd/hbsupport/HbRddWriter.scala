package top.spoofer.hbrdd.hbsupport

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD
import top.spoofer.hbrdd.config.HbRddConfig
import top.spoofer.hbrdd.unit.HbRddFormatsWriter
import top.spoofer.hbrdd._
import HbRddWritPuter._

trait HbRddWriter {
  type TsValue[A] = (Long, A) // (ts, A)
  val LATEST_TIMESTAMP = Long.MaxValue
  /**
   * (rowID, Map[family, Map[qualifier, value]])
   * @param rdd rdd
   * @param writer 格式化写入
   * @tparam A 类型参数
   * @return
   */
  implicit def rdd2Hbase[A](rdd: RDD[(String, Map[String, Map[String, A]])])(implicit writer: HbRddFormatsWriter[A]): RDDWriter[A] = {
    new RDDWriter(rdd, hbRddSetPuter[A])
  }

  /**
   * (rowID, Map[family, Map[qualifier, (ts, value)]])
   * @param rdd rdd
   * @param writer 格式化写入
   * @tparam A 类型参数
   * @return
   */
  implicit def rdd2HbaseUseTs[A](rdd: RDD[(String, Map[String, Map[String, TsValue[A]]])])(implicit writer: HbRddFormatsWriter[A]): RDDWriter[TsValue[A]] = {
    new RDDWriter(rdd, hbRddSetTsPuter[A])
  }

  implicit def singleFamilyRdd2Hbase[A](rdd: RDD[(String, Map[String, A])])(implicit writer: HbRddFormatsWriter[A]): SingleFamilyRDDWriter[A] = {
    new SingleFamilyRDDWriter(rdd, hbRddSetPuter[A])
  }

  implicit def singleFamilyRdd2HbaseTs[A](rdd: RDD[(String, Map[String, TsValue[A]])])(implicit writer: HbRddFormatsWriter[A]): SingleFamilyRDDWriter[TsValue[A]] = {
    new SingleFamilyRDDWriter(rdd, hbRddSetTsPuter[A])
  }
}

/**
 * 用来设置put
 */
private[hbrdd] object HbRddWritPuter {
  type HbRddPuter[A] = (Put, Array[Byte], Array[Byte], A) => Put

  def hbRddSetPuter[A](puter: Put, family: Array[Byte], qualifier: Array[Byte],
    value: A)(implicit writer: HbRddFormatsWriter[A]): Put = {
    puter.addColumn(family, qualifier, writer.formatsWrite(value))
  }

  //需要设置时间戳时使用
  def hbRddSetTsPuter[A](puter: Put, family: Array[Byte], qualifier: Array[Byte],
    value: TsValue[A])(implicit writer: HbRddFormatsWriter[A]): Put = {
    puter.addColumn(family, qualifier, value._1, writer.formatsWrite(value._2))
  }
}

sealed abstract class HbRddWritCommon[A] {
  protected def convert2Writable(rowId: String, data: Map[String, Map[String, A]],
    puter: HbRddPuter[A]): Option[(ImmutableBytesWritable, Put)] = {
    val put = new Put(rowId)

    for {
      (family, columnContents) <- data
      (qualifier, value) <- columnContents
    } {
      puter(put, family, qualifier, value)
    }

    if (put.isEmpty) None else Some(new ImmutableBytesWritable, put)
  }
}

/**
 * (rowID, Map[family, Map[qualifier, value]])
 * @param rdd rdd
 * @param put 写入函数
 * @tparam A 类型参数
 */
final class RDDWriter[A](
  val rdd: RDD[(String, Map[String, Map[String, A]])],
    val put: HbRddPuter[A]
) extends HbRddWritCommon[A] with Serializable {
  def put2Hbase(tableName: String)(implicit config: HbRddConfig) = {
    val job = createJob(tableName, config.getHbaseConfig)
    rdd.flatMap({ case (rowId, data) => convert2Writable(rowId, data, put) })
      .saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}

/**
 * 数据表只有一个family
 * (rowId, Map[qualifier, value]) => (rowID, Map[family, Map[qualifier, value]])
 * @param rdd rdd
 * @param put 写入函数
 * @tparam A 类型参数
 */
final class SingleFamilyRDDWriter[A](
  val rdd: RDD[(String, Map[String, A])],
    val put: HbRddPuter[A]
) extends HbRddWritCommon[A] with Serializable {
  def put2Hbase(tableName: String, family: String)(implicit config: HbRddConfig) = {
    val job = createJob(tableName, config.getHbaseConfig)
    rdd.flatMap({ case (rowId, data) => convert2Writable(rowId, Map(family -> data), put) })
      .saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}
