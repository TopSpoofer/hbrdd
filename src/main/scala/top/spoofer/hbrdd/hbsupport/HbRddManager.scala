package top.spoofer.hbrdd.hbsupport

import java.text.SimpleDateFormat

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.spark.rdd.RDD
import top.spoofer.hbrdd.config.HbRddConfig


trait HbRddManager {
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

    /**
      * 创建设定列簇的表, 如果表存在,不进行任何操作,也不会抛出异常
      * @param tableName 表名字
      * @param families 列簇
      * @param splitKeys 定义region splits的keys
      * @return
      */
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

    /**
      * 创建设定列簇属性的表, 如果表存在,不进行任何操作,也不会抛出异常
      * @param tableName 表名字
      * @param propertieFamilies 配置了属性的family描述符
      * @param splitKeys 定义region splits的keys
      * @return
      */
    def createTableByProperties(tableName: String, propertieFamilies: TraversableOnce[HbRddFamily],
                    splitKeys: TraversableOnce[String]): HbRddAdmin = {
      val table = TableName.valueOf(tableName)
      val admin = connection.getAdmin

      if (!admin.tableExists(table) && !admin.isTableAvailable(table)) {
        val tableDesc = new HTableDescriptor(table)

        propertieFamilies foreach { family =>
          tableDesc.addFamily(family)
        }

        if (splitKeys != null || splitKeys.nonEmpty) {
          admin.createTable(tableDesc, splitKeys map { Bytes.toBytes } toArray)
        } else admin.createTable(tableDesc)
      }
      this
    }

    def createTableByProperties(tableName:String, family: HbRddFamily, splitKeys: TraversableOnce[String]): HbRddAdmin = {
      this.createTableByProperties(tableName, Set(family), splitKeys)
    }

    def createTableByProperties(tableName: String, families: TraversableOnce[HbRddFamily]): HbRddAdmin = {
      this.createTableByProperties(tableName, families, List.empty)
    }

    def createTableByProperties(tableName: String, families: HbRddFamily*): HbRddAdmin = {
      this.createTableByProperties(tableName, families.toSet, List.empty)
    }

    def createTableByProperties(tableName: String, splitKeys: TraversableOnce[String], families: HbRddFamily*): HbRddAdmin = {
      this.createTableByProperties(tableName, families.toSet, splitKeys)
    }

    /**
      * 在数据表中加入新的family, 这里不再像cretae table那样提供string的接口
      * 如果不能成功执行,将抛出异常
      * @param tableName 表名字
      * @param families 要加入的列
      * @return
      */
    def addFamilies(tableName: String, families: TraversableOnce[HbRddFamily]): HbRddAdmin = {
      val table = TableName.valueOf(tableName)
      val admin = connection.getAdmin

      if (admin.tableExists(table)) {
        try {
          admin.disableTable(table)
          val tableDescriptor = admin.getTableDescriptor(table)
          families foreach { cf =>
            if (!tableDescriptor.hasFamily(cf.name.getBytes)) tableDescriptor.addFamily(cf)
          }
          admin.modifyTable(table, tableDescriptor)
        } catch {
          case ex: Exception => throw ex
        } finally {
          //不管怎么样都enable table
          admin.enableTable(table)
        }
      }
      this
    }

    def addFamily(tableName: String, family: HbRddFamily): HbRddAdmin = {
      this.addFamilies(tableName, Set(family))
    }

    def addFamilies(tableName: String, families: HbRddFamily*): HbRddAdmin = {
      this.addFamilies(tableName, families.toSet)
    }

    /**
      * 在表中删除列簇, 如果不成功会抛出异常
      * @param tableName 表名字
      * @param families 要删除的列簇
      * @return
      */
    def deleteFamilies(tableName: String, families: TraversableOnce[HbRddFamily]): HbRddAdmin = {
      val table = TableName.valueOf(tableName)
      val admin = connection.getAdmin

      if (admin.tableExists(table)) {
        try {
          admin.disableTable(table)
          val tableDescriptor = admin.getTableDescriptor(table)
          families foreach { cf =>
            if (tableDescriptor.hasFamily(cf.name.getBytes)) tableDescriptor.removeFamily(cf.name.getBytes)
          }
          admin.modifyTable(table, tableDescriptor)
        } catch {
          case ex: Exception => throw ex
        } finally {
          admin.enableTable(table)
        }
      }
      this
    }

    def deleteFamily(tableName: String, family: HbRddFamily): HbRddAdmin = {
      this.deleteFamilies(tableName, Set(family))
    }

    def deleteFamilies(tableName: String, families: HbRddFamily*): HbRddAdmin = {
      this.deleteFamilies(tableName, families.toSet)
    }

    def deleteFamilyByName(tableName: String, family: String): HbRddAdmin = {
      this.deleteFamilies(tableName, Set(HbRddFamily(family)))
    }

    def deleteFamiliesByName(tableName: String, families: String*): HbRddAdmin = {
      val cfsStr = families.toSet
      val cfs = cfsStr.map(cf => HbRddFamily(cf))
      this.deleteFamilies(tableName, cfs.toSet)
    }

    /**
      * 更新hbase数据表的属性, 如果不成功，抛出异常
      * 如果表中不包含指定的列簇,不会创建列簇也不会抛出异常,只是简单地不对其进行操作
      * @param tableName 表名字
      * @param families 列簇
      * @return
      */
    def updateFamilies(tableName: String, families: TraversableOnce[HbRddFamily]): HbRddAdmin = {
      val table = TableName.valueOf(tableName)
      val admin = connection.getAdmin
      if (admin.tableExists(table)) {
        try {
          admin.disableTable(table)
          val tableDescriptor = admin.getTableDescriptor(table)
          families foreach { cf =>
            if (tableDescriptor.hasFamily(cf.name.getBytes)) tableDescriptor.modifyFamily(cf)
          }
          admin.modifyTable(table, tableDescriptor)
        } catch {
          case ex: Exception => throw ex
        } finally {
          admin.enableTable(table)
        }
      }
      this
    }

    def updateFamily(tableName: String, family: HbRddFamily): HbRddAdmin = {
      this.updateFamilies(tableName, Set(family))
    }

    def updateFamilies(tableName: String, families: HbRddFamily*): HbRddAdmin = {
      this.updateFamilies(tableName, families.toSet)
    }

    /**
      * 使数据表变为可用
      * @param tableName 表名字
      * @return
      */
    def enableTable(tableName: String): HbRddAdmin = {
      val table = TableName.valueOf(tableName)
      val admin = connection.getAdmin
      if (admin.tableExists(table)) admin.enableTable(table)
      this
    }

    /**
      * 禁用一个数据表
      * @param tableName 数据表的名字
      * @param requireExists 如果为true, 当表不存在的时候会抛出异常
      * @return
      */
    def disableTable(tableName: String, requireExists: Boolean = false): HbRddAdmin = {
      val table = TableName.valueOf(tableName)
      val admin = connection.getAdmin

      if (requireExists) {
        require(admin.tableExists(table), s"table $tableName not exists")
        admin.disableTable(table)
      } else {
        if (admin.tableExists(table)) admin.disableTable(table)
      }
      this
    }

    /**
      * 删除数据表, 在进行删除前需要disabletable, 否则会抛出异常
      * 这是一个通用的函数， 如果要直接删除表, 使用dropTable
      * @param tableName 表名字
      * @return
      */
    def deleteTable(tableName: String): HbRddAdmin = {
      val table = TableName.valueOf(tableName)
      val admin = connection.getAdmin

      if (admin.tableExists(table)) admin.deleteTable(table)
      this
    }

    /**
      * 先 disable 表, 再delete table
      * @param tableName 表名字
      * @return
      */
    def dropTable(tableName: String): HbRddAdmin = {
      this.disableTable(tableName)
      this.deleteTable(tableName)
    }

    /**
      * 先禁止table再截断
      * @param tableName 表名字
      * @param preserveSplits 是否保存分裂
      * @return
      */
    def truncateTable(tableName: String, preserveSplits: Boolean): HbRddAdmin = {
      val table = TableName.valueOf(tableName)
      val admin = connection.getAdmin
      if (admin.tableExists(table)) {
        admin.disableTable(table)
        admin.truncateTable(table, preserveSplits)
      }
      this
    }

    /**
      * 产生一个table快照
      * @param tableName 表名字
      * @param snapshotName 快照名字
      * @return
      */
    def tableSnapshot(tableName: String, snapshotName: String): HbRddAdmin = {
      val table = TableName.valueOf(tableName)
      val tableDesc = new HTableDescriptor(table)
      val admin = connection.getAdmin
      admin.snapshot(snapshotName, tableDesc.getTableName)
      this
    }

    /**
      * 产生一个table快照, 使用默认的快照名字${tableName}_${yyyy-MM-dd-HHmmss}
      * @param tableName 表名字
      * @return
      */
    def tableSnapshot(tableName: String): HbRddAdmin = {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd-HHmmss")
      val suffix = dateFormat.format(System.currentTimeMillis())
      this.tableSnapshot(tableName, s"${tableName}_$suffix")
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
