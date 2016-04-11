
import top.spoofer.hbrdd._
import top.spoofer.hbrdd.config.HbRddConfig
import top.spoofer.hbrdd.hbsupport.{HbRddFamily, FamilyPropertiesStringSetter}

object TestHbAdmin {
  val tableName = "test_hbrdd"
  implicit val hbConfig = HbRddConfig()

  private def createtable() = {
    val hh = HbRddAdmin.apply()
    val l = List("ooooo", "iiiii")  //split keys
    hh.createTable(tableName, l, "cf1", "cf2")
    hh.close()
  }

  private def getPropertiesFamilys = {
    val map1 = Map("maxversions" -> "1000", "ttl" -> "9999", "inmem" -> "false")
    val fp1 = FamilyPropertiesStringSetter(map1)
    val cf1 = HbRddFamily("ffff", fp1)

    val map2 = Map("maxversions" -> "1000", "minversions" -> "2", "ttl" -> "9999", "blocksize" -> "222",
      "inmem" -> "true", "bloomfilter" -> "rowcol", "scope" -> "1", "keepdeletecells" -> "false", "blockcache" -> "false")

    val fp2 = FamilyPropertiesStringSetter(map2)
    val cf2 = HbRddFamily("eeee", fp2)

    println(cf2.getKeepDeletedCells.toString)

    Set(cf1, cf2)
  }

  private def createtableByProperties() = {
    val hh = HbRddAdmin.apply()
    val l = List("ooooo", "iiiii")  //split keys
    hh.createTableByProperties("testProperties", this.getPropertiesFamilys, l)
    hh.close()
  }

  private def deleteTable() = {
    val hh = HbRddAdmin.apply()
    hh.dropTable(tableName)
    hh.close()
  }

  private def truncateTable() = {
    val hh = HbRddAdmin.apply()
    hh.truncateTable(tableName, preserveSplits = true)
    hh.close()
  }

  private def tableSnapshot() = {
    val hh = HbRddAdmin.apply()
    hh.tableSnapshot(tableName)
    hh.close()
  }

  def main(args: Array[String]) {
//    println(hh)
//    println("===")
//    this.tableSnapshot()
//    this.createtable()
//    this.truncateTable()
//    this.deleteTable()

    this.createtableByProperties()
  }
}
