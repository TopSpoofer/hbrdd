package top.spoofer.hbrdd.hbsupport

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD
import top.spoofer.hbrdd.config.HbRddConfig
import top.spoofer.hbrdd.unit.HbRddFormatsWriter
import top.spoofer.hbrdd._
import HbRddWritPuter._

trait HbRddWriter {
  val LATEST_TIMESTAMP = Long.MaxValue
  /**
    * (rowID, Map[family, Map[qualifier, value]])
    * @param rdd rdd
    * @param writer 格式化写入
    * @tparam A 类型参数
    * @return
    */
  implicit def rdd2Hbase[A](rdd: RDD[(String, Map[String, Map[String, A]])])
                           (implicit writer: HbRddFormatsWriter[A]): RDDWriter[A] = {
    new RDDWriter(rdd, hbRddSetPuter[A], LATEST_TIMESTAMP)
  }
}

/**
  * 用来设置put
  */
private[hbrdd] object HbRddWritPuter {
  type HbRddPut[A] = (Put, Array[Byte], Array[Byte], A)   //(put, family, qualifier, value)
  type HbRddPuter[A] = (HbRddPut[A], Long) => Put

//  def hbRddSetPuter[A](puter: HbRddPut[A])(implicit writer: HbRddFormatsWriter[A]): Put = {
//    val put = puter._1
//    put.addColumn(puter._2, puter._3, writer.formatsWrite(puter._4))
//  }

  //需要设置时间戳时使用
  def hbRddSetPuter[A](puter: HbRddPut[A], ts: Long)(implicit writer: HbRddFormatsWriter[A]): Put = {
    val put = puter._1
    put.addColumn(puter._2, puter._3, ts, writer.formatsWrite(puter._4))
  }
}

sealed abstract class HbRddWritCommon[A] {
  protected def convert2Writable(rowId: String, datas: Map[String, Map[String, A]],
                                 puter: HbRddPuter[A], ts: Long): Option[(ImmutableBytesWritable, Put)] = {
    val put = new Put(rowId)

    for {
      (family, columnContent) <- datas
      (qualifier, value) <- columnContent
    } {
      val hbRddPut: HbRddPut[A] = (put, family, qualifier, value)
      puter(hbRddPut, ts)
    }

    if (put.isEmpty) None else Some(new ImmutableBytesWritable, put)
  }


//

}

/**
  * (rowID, Map[family, Map[qualifier, value]])
  * @param rdd rdd
  * @param put 写入函数
  * @tparam A 类型参数
  */
final class RDDWriter[A](val rdd: RDD[(String, Map[String, Map[String, A]])],
                         val put: HbRddPuter[A], ts: Long) extends HbRddWritCommon[A] with Serializable {
  def put2Hbase(tableName: String)(implicit config: HbRddConfig) = {
    val job = createJob(tableName, config.getHbaseConfig)
    rdd.flatMap({ case (rowId, datas) => convert2Writable(rowId, datas, put, ts) })
      .saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}

//final class OneFamilyRDDWriter[A](val rdd: RDD[(String, Map[String, A])], val put: )