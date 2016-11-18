package top.spoofer.hbrdd.hbsupport

import java.util.UUID

import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{ KeyValue, TableName }
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.spark.Partitioner
import top.spoofer.hbrdd.unit.HbRddFormatsWriter
import HFileHelper._
import CellKeyCompare._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapreduce.{ HFileOutputFormat2, LoadIncrementalHFiles }
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import top.spoofer.hbrdd.config.HbRddConfig

import scala.reflect.ClassTag
import scala.collection.JavaConversions._

/**
 * Created by spoofer on 16-6-21.
 */
trait HbRddBulker {
  implicit lazy val cellkeyCmp = new CellKeyCMP
  implicit lazy val cellKeyCmpTs = new CellKeyCMP4Ts

  implicit def toSimpleHFileRDD[A: ClassTag](rdd: RDD[(String, Map[String, A])])(implicit writer: HbRddFormatsWriter[A]): SimpleHFileRdd[CellKey, A, A] = {
    new SimpleHFileRdd[CellKey, A, A](rdd, getCellKey[A], familyWarpper[A])
  }

  implicit def toSimpleHFileRDDTs[A: ClassTag](rdd: RDD[(String, Map[String, (Long, A)])])(implicit writer: HbRddFormatsWriter[A]): SimpleHFileRdd[CellKeyTs, (Long, A), A] = {
    new SimpleHFileRdd[CellKeyTs, (Long, A), A](rdd, getCellKey[A], familyWarpper4Ts[A])
  }

  implicit def toHFileRDD[A: ClassTag](rdd: RDD[(String, Map[String, Map[String, A]])])(implicit writer: HbRddFormatsWriter[A]): HFileRdd[CellKey, A, A] = {
    new HFileRdd[CellKey, A, A](rdd, getCellKey[A], familyWarpper[A])
  }

  implicit def toHFileRDDTs[A: ClassTag](rdd: RDD[(String, Map[String, Map[String, (Long, A)]])])(implicit writer: HbRddFormatsWriter[A]): HFileRdd[CellKeyTs, (Long, A), A] = {
    new HFileRdd[CellKeyTs, (Long, A), A](rdd, getCellKey[A], familyWarpper4Ts[A])
  }
}

private[hbrdd] object HFileHelper {
  //每个单元格的key： (keyid, qualifier) 或者 (CellKey, ts)
  type CellKey = (Array[Byte], Array[Byte])
  type CellKeyTs = (CellKey, Array[Byte])

  type CellKeyObtain[C, A, V] = (CellKey, A) => (C, V)
  type KeyValueWrap[C, V] = (C, V) => (ImmutableBytesWritable, KeyValue)
  type KeyValueWrapper4Family[C, V] = (Array[Byte]) => KeyValueWrap[C, V]

  def getCellKey[A](cellkey: CellKey, value: A) = (cellkey, value)
  def getCellKey[A](cellkey: CellKey, valueTs: (Long, A)) = ((cellkey, Bytes.toBytes(valueTs._1)), valueTs._2)

  def familyWarpper[A](family: Array[Byte])(cellkey: CellKey, value: A)(implicit writer: HbRddFormatsWriter[A]) =
    (new ImmutableBytesWritable(cellkey._1), new KeyValue(cellkey._1, family, cellkey._2, writer.formatsWrite(value)))

  def familyWarpper4Ts[A](family: Array[Byte])(cellKeyTs: CellKeyTs, value: A)(implicit writer: HbRddFormatsWriter[A]) =
    (new ImmutableBytesWritable(cellKeyTs._1._1), new KeyValue(cellKeyTs._1._1, family, cellKeyTs._1._2, Bytes.toLong(cellKeyTs._2), writer.formatsWrite(value)))
}

private[hbrdd] object CellKeyCompare {
  class CellKeyCMP extends Ordering[CellKey] {
    override def compare(c1: CellKey, c2: CellKey) = {
      val (ckey1, cqual1) = c1
      val (ckey2, cqual2) = c2

      val ret = Bytes.compareTo(ckey1, ckey2)
      if (ret != 0) ret
      else Bytes.compareTo(cqual1, cqual2)
    }
  }

  class CellKeyCMP4Ts extends Ordering[CellKeyTs] {
    val cellKeyCMP = new CellKeyCMP()

    @inline
    private def tsCompare(ts1: Long, ts2: Long) = {
      if (ts1 < ts2) 1 //ts越小, 数据越早产生
      else if (ts1 > ts2) -1
      else 0
    }

    override def compare(c1: CellKeyTs, c2: CellKeyTs) = {
      val (cell1, ts1) = c1
      val (cell2, ts2) = c2

      val ret = cellKeyCMP.compare(cell1, cell2)
      if (ret != 0) ret
      else tsCompare(Bytes.toLong(ts1), Bytes.toLong(ts2))
    }
  }
}

sealed abstract class HFileCommon extends Serializable {
  protected abstract class HbRddPartitioner extends Partitioner {
    def extractKey(cellKey: Any) = cellKey match { // CellKey or CellKeyTs
      case (key: Array[Byte], _) => key
      case ((key: Array[Byte], _), _) => key
    }
  }

  private object HbRddPartitioner {
    def apply(config: Configuration, splits: Array[Array[Byte]], numHFilesPerRegionPerFamily: Int): HbRddPartitioner = {
      if (numHFilesPerRegionPerFamily == 1) new SinglePartitioner(splits)
      else {
        val fraction = (1 max numHFilesPerRegionPerFamily) min config.getInt(LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY, 32)
        new MultiplePartitioner(splits, fraction)
      }
    }
  }

  private class SinglePartitioner(splits: Array[Array[Byte]]) extends HbRddPartitioner {
    override def numPartitions: Int = splits.length

    override def getPartition(cellkey: Any): Int = {
      val key = extractKey(cellkey)
      for (i <- 1 until splits.length) {
        if (Bytes.compareTo(key, splits(i)) < 0) return i - 1
      }
      splits.length - 1
    }
  }

  private class MultiplePartitioner(splits: Array[Array[Byte]], fraction: Int) extends HbRddPartitioner {
    override def numPartitions: Int = splits.length

    override def getPartition(cellkey: Any): Int = {
      val key = extractKey(cellkey)
      val hc = (key.hashCode() & (Int.MaxValue >> 2)) % fraction
      for (i <- 1 until splits.length) {
        if (Bytes.compareTo(key, splits(i)) < 0) return (i - 1) * fraction + hc
      }
      (splits.length - 1) * fraction + hc
    }
  }

  protected def getPartitioner(regionLocator: RegionLocator, numHFilesPerRegionPerFamily: Int)(implicit config: HbRddConfig) = {
    HbRddPartitioner(config.getHbaseConfig, regionLocator.getStartKeys, numHFilesPerRegionPerFamily)
  }

  protected def getPartitionedRDD[C: ClassTag, A: ClassTag](rdd: RDD[(C, A)], keyvalueWrap: KeyValueWrap[C, A], partitioner: HbRddPartitioner)(implicit cmp: Ordering[C]) = {
    rdd.repartitionAndSortWithinPartitions(partitioner).map { case (cell, value) => keyvalueWrap(cell, value) }
  }

  @inline
  private def hFilePath(table: Table) = new Path("/tmp", table.getName.getQualifierAsString + "_" + UUID.randomUUID())

  protected def saveAsHFile(rdd: RDD[(ImmutableBytesWritable, KeyValue)], table: Table, regionLocator: RegionLocator, connection: Connection)(implicit config: HbRddConfig) = {
    val job = Job.getInstance(config.getHbaseConfig, this.getClass.getName.split('$')(0))
    HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator)

    val fileSystem = FileSystem.get(config.getHbaseConfig)
    val hfPath = hFilePath(table)
    fileSystem.makeQualified(hfPath)

    try {
      rdd.saveAsNewAPIHadoopFile(hfPath.toString, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], job.getConfiguration)
      val permission = new FsPermission("776")

      def setPermissionRecursively(path: Path): Unit = {
        val fileList = fileSystem.listStatus(path)
        fileList foreach { file =>
          val fp = file.getPath
          fileSystem.setPermission(fp, permission)
          if (fp.getName != "_tmp" && file.isDirectory) {
            FileSystem.mkdirs(fileSystem, new Path(fp, "_tmp"), permission)
            setPermissionRecursively(fp)
          }
        }
      }

      setPermissionRecursively(hfPath)
      val lih = new LoadIncrementalHFiles(config.getHbaseConfig)
      lih.doBulkLoad(hfPath, connection.getAdmin, table, regionLocator)
    } finally {
      connection.close()
      fileSystem.deleteOnExit(hfPath)
      //清理 HFileOutputFormat2 进行stuff 时留下的文件
      fileSystem.deleteOnExit(new Path(TotalOrderPartitioner.getPartitionFile(job.getConfiguration)))
    }
  }
}

/**
 * 单family的情况
 * @param rdds (String, Map[String, A]) => (rowID, Map[qualifier, value])
 * @param cko CellKeyObtain
 * @param familyWrapper KeyValueWrapper4Family
 */
final class SimpleHFileRdd[C: ClassTag, A: ClassTag, V: ClassTag](rdds: RDD[(String, Map[String, A])], cko: CellKeyObtain[C, A, V],
    familyWrapper: KeyValueWrapper4Family[C, V]) extends HFileCommon {
  def saveToHbaseByBulk(table: String, family: String, numHFilesPerRegionPerFamily: Int = 1)(implicit config: HbRddConfig, cmp: Ordering[C]): Unit = {
    require(numHFilesPerRegionPerFamily > 0)

    val tableName = TableName.valueOf(table)
    val connection = ConnectionFactory.createConnection(config.getHbaseConfig)
    val regionLocator = connection.getRegionLocator(tableName)
    val hbTable = connection.getTable(tableName)

    val partitioner = getPartitioner(regionLocator, numHFilesPerRegionPerFamily)
    val rdd = rdds.flatMap {
      case (k, m) =>
        val key = Bytes.toBytes(k)
        m map { case (h, v) => cko((key, Bytes.toBytes(h)), v) }
    }

    saveAsHFile(getPartitionedRDD(rdd, familyWrapper(Bytes.toBytes(family)), partitioner), hbTable, regionLocator, connection)
  }
}

/**
 * 多family的情况
 * @param mapRdds  (String, Map(String, Map(String, A))) => (rowID, Map(familyKey, Map(qualifier, Value)))
 * @param cko CellKeyObtain
 * @param familyWrapper KeyValueWrapper4Family
 */
final class HFileRdd[C: ClassTag, A: ClassTag, V: ClassTag](
  mapRdds: RDD[(String, Map[String, Map[String, A]])],
    cko: CellKeyObtain[C, A, V], familyWrapper: KeyValueWrapper4Family[C, V]
) extends HFileCommon {
  def saveToHbaseByBulk(table: String, numHFilesPerRegionPerFamily: Int = 1)(implicit config: HbRddConfig, cmp: Ordering[C]): Unit = {
    require(numHFilesPerRegionPerFamily > 0)

    val tableName = TableName.valueOf(table)
    val connection = ConnectionFactory.createConnection(config.getHbaseConfig)
    val regionLocator = connection.getRegionLocator(tableName)
    val hTable = connection.getTable(tableName)
    val families = hTable.getTableDescriptor.getFamiliesKeys map Bytes.toString
    val partitioner = getPartitioner(regionLocator, numHFilesPerRegionPerFamily)

    val rdds = for {
      f <- families
      rdd = mapRdds.collect { case (k, m) if m.contains(f) => (Bytes.toBytes(k), m(f)) }
        .flatMap { case (k, m) => m map { case (h, v) => cko((k, Bytes.toBytes(h)), v) } }
    } yield getPartitionedRDD(rdd, familyWrapper(Bytes.toBytes(f)), partitioner)

    saveAsHFile(rdds.reduce(_ ++ _), hTable, regionLocator, connection)
  }
}
