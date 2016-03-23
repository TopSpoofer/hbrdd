package top.spoofer.hbrdd.hbsupport

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.spark.rdd.RDD
import top.spoofer.hbrdd.config.HbRddConfig


trait HbRddManager {
  implicit def arrayStringToArrayBytes(array: Array[String]): Array[Array[Byte]]  = array map { Bytes.toBytes }
  implicit def stringToBytes(str: String): Array[Byte] = Bytes.toBytes(str)

  class HbRddAdmin(connection: Connection) {
    /**
      * 判断一个表和其列簇是否存在
      * @param tableName 表名字
      * @param family 列簇
      * @return 存在返回true否则false
      */
    def tableExists(tableName: String, family: String): Boolean = {
      val table = TableName.valueOf(tableName)
      val admin = connection.getAdmin
      if (!admin.tableExists(table)) false
      else {
        val families = admin.getTableDescriptor(table).getFamiliesKeys
        require(families.contains(family.getBytes()), s"table $tableName exists but family $family not found !")
        true
      }
    }

    def tableExists(tableName: String, families: TraversableOnce[String]): Boolean = {
      val table = TableName.valueOf(tableName)
      val admin = connection.getAdmin
      if (!admin.tableExists(table)) false
      else {
        val tableFamilies = admin.getTableDescriptor(table).getFamiliesKeys
        for (family <- families)
          require(tableFamilies.contains(family.getBytes()), s"table $tableName exists but falimy $family not found !")
        true
      }
    }

    def tableExists(tableName: String, families: String*): Boolean = {
      this.tableExists(tableName, families.toList)
    }

    /**
      * 这个不会发生异常，也不会判断表中的列簇是否存在
      * @param tableName 表名字
      * @return 如果表存在(不管列簇)返回true否则返回false
      */
    def tableExists(tableName: String): Boolean = {
      val table = TableName.valueOf(tableName)
      val admin = connection.getAdmin
      admin.tableExists(table)
    }

    def createTable(tableName: String, families: TraversableOnce[String],
                    splitKeys: TraversableOnce[String]): HbRddAdmin = {
      val table = TableName.valueOf(tableName)
      val admin = connection.getAdmin

      if (!admin.tableExists(table) && !admin.isTableAvailable(table)) {
        val tableDesc = new HTableDescriptor(table)

        families foreach { family =>
          tableDesc.addFamily(new HColumnDescriptor(family))
        }

        if (splitKeys != null || splitKeys.nonEmpty) {
          admin.createTable(tableDesc, splitKeys map { Bytes.toBytes } toArray)
        } else admin.createTable(tableDesc)
      }
      this
    }

    def createTable(tableName:String, family: String, splitKeys: TraversableOnce[String]): HbRddAdmin = {
      this.createTable(tableName, Set(family), splitKeys)
    }

    def createTable(tableName: String, families: TraversableOnce[String]): HbRddAdmin = {
      this.createTable(tableName, families, List.empty)
    }

    def createTable(tableName: String, families: String*): HbRddAdmin = {
      this.createTable(tableName, families.toSet, List.empty)
    }

    def createTable(tableName: String, splitKeys: TraversableOnce[String], families: String*): HbRddAdmin = {
      this.createTable(tableName, families.toSet, splitKeys)
    }

    def close() = this.connection.close()
  }

  object HbRddAdmin {
    def apply()(implicit config: HbRddConfig): HbRddAdmin = {
      new HbRddAdmin(ConnectionFactory.createConnection(config.getHbaseConfig))
    }
  }

  /**
    * 创建一个mr job
    * @param table 表名字
    * @param conf 配置
    * @return 返回一个mr job 对象
    */
  protected[hbrdd] def createJob(table: String, conf: Configuration): Job = {
    conf.set(TableOutputFormat.OUTPUT_TABLE, table)
    val job = Job.getInstance(conf, this.getClass.getName.split('$')(0))
    job.setOutputFormatClass(classOf[TableOutputFormat[String]])
    job
  }

  /**
    * 利用排序使rdd得到分区数跟regions数一样的rdd,然后取每个regions的第一个elem, 最后把第一个region的key去掉
    * 第一个region分区的key是不需要的, 因为第一个分区不确定会split
    * @param rdd rdd
    * @param regionsAmount  regions 数量
    * @return
    */
  def rddSplit(rdd: RDD[String], regionsAmount: Int): Seq[String] = {
    rdd.sortBy(elem => elem, numPartitions = regionsAmount).mapPartitions(p => p.take(1)).collect().toList.tail
  }
}
