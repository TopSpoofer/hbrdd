package top.spoofer.hbrdd.hbsupport

import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD
import top.spoofer.hbrdd.config.HbRddConfig
import top.spoofer.hbrdd.hbsupport.HbRddDeleterTools._
import top.spoofer.hbrdd._


trait HbRddDeleter {
  implicit def rowKeyRDDDelete(rdd: RDD[String]): RowKeyRDDDelete = new RowKeyRDDDelete(rdd, deleter)
}

private[hbrdd] object HbRddDeleterTools {
  type DeleteTool[A] = (Delete, Array[Byte], A) => Delete

  def deleter(delete: Delete, family: Array[Byte], qualifier: String) = delete.addColumn(family, qualifier)
  def deleter(delete: Delete, family: Array[Byte], tsQualifier: (Long, String)) = {
    delete.addColumn(family, tsQualifier._2, tsQualifier._1)
  }
}

sealed abstract class HbRddDeleteCommon[A] {
  /**
    *
    * @param rowId 行id
    * @param data Map(family, Set(qualifier))
    * @param deleter DeleteTool
    * @return
    */
  protected def convert2Delete(rowId: String, data: Map[String, Set[A]],
                               deleter: DeleteTool[A]): Option[(ImmutableBytesWritable, Delete)] = {
    val delete = new Delete(rowId)
    for {
      (family, columnContents) <- data
      content <- columnContents
    } {
      deleter(delete, family, content)
    }

    if (delete.isEmpty) None else Some(new ImmutableBytesWritable, delete)
  }

  protected def convert2Delete(rowId: String, families: Set[String]) = {
    val delete = new Delete(rowId)
    for (family <- families) delete.addFamily(family)
    if (delete.isEmpty) None else Some(new ImmutableBytesWritable, delete)
  }

  protected def convert2Delete(rowId: String): Option[(ImmutableBytesWritable, Delete)] = {
    val delete = new Delete(rowId)
    Some(new ImmutableBytesWritable, delete)
  }
}

/**
  * @param rdd (rowId, Map(family, Set(qualifier or (ts, qualifier))))
  * @param deleter 构建Delete的函数
  * @tparam A 类型参数
  */
final class RDDDelete[A](val rdd: RDD[(String, Map[String, Set[A]])],
                         val deleter: DeleteTool[A]) extends HbRddDeleteCommon[A] with Serializable {
  def deleteHbase(table: String)(implicit config: HbRddConfig) = {
    val job = createJob(table, config.getHbaseConfig)
    rdd.flatMap({ case (rowId, data) => this.convert2Delete(rowId, data, deleter) })
      .saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}

/**
  *
  * @param rdd (rowId, Set(qualifier or (ts, qualifier)))
  * @param deleter 构建Delete的函数
  * @tparam A 类型参数
  */
final class SingleFamilyRDDDelete[A](val rdd: RDD[(String, Set[A])],
                                     val deleter: DeleteTool[A]) extends HbRddDeleteCommon[A] with Serializable {
  def deleteHbase(table: String, family: String)(implicit config: HbRddConfig) = {
    val job = createJob(table, config.getHbaseConfig)
    rdd.flatMap({ case (rowId, qualifiers) => this.convert2Delete(rowId, Map(family -> qualifiers), deleter) })
      .saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}

final class RowKeyRDDDelete(val rdd: RDD[String],
                               val deleter: DeleteTool[String]) extends  HbRddDeleteCommon[String] with Serializable {
  /**
    * 删除行数据
    * @param table 数据表
    * @param config 配置
    */
  def deleteHbase(table: String)(implicit config: HbRddConfig) = {
    val job = createJob(table, config.getHbaseConfig)
    rdd.flatMap({ rowId => this.convert2Delete(rowId) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  /**
    * 删除行数据, 指定列簇
    * @param table hbase 表
    * @param families 列簇
    * @param config 配置
    */
  def deleteHbase(table: String, families: Set[String])(implicit config: HbRddConfig) = {
    val job = createJob(table, config.getHbaseConfig)
    rdd.flatMap({ rowId => this.convert2Delete(rowId, families) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  /**
    * 删除行数据, 指定的列簇中的列
    * @param table hbase 表
    * @param family family
    * @param qualifiers 列名字
    * @param config 配置
    */
  def deleteHbase(table: String, family: String, qualifiers: Set[String])(implicit config: HbRddConfig) = {
    val job = createJob(table, config.getHbaseConfig)
    rdd.flatMap({ rowId => this.convert2Delete(rowId, Map(family -> qualifiers), deleter) })
      .saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  /**
    * 删除行数据, 指定的列簇中的列
    * @param table hbase 表
    * @param data map(family, Set(qualifier))
    * @param config 配置
    */
  def deleteHbase(table: String, data: Map[String, Set[String]])(implicit config: HbRddConfig) = {
    val job = createJob(table, config.getHbaseConfig)
    rdd.flatMap({ rowId => this.convert2Delete(table, data, deleter) })
      .saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}
