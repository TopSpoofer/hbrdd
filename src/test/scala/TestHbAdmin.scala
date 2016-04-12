
import top.spoofer.hbrdd._
import top.spoofer.hbrdd.config.HbRddConfig
import top.spoofer.hbrdd.hbsupport.{HbRddFamily, FamilyPropertiesStringSetter}

object TestHbAdmin {
  val tableName = "test_hbrdd"
  implicit val hbConfig = HbRddConfig()

  private def createtable() = {
    val admin = HbRddAdmin.apply()
    val splitkeys = List("ooooo", "iiiii")  //split keys
    admin.createTable(tableName, splitkeys, "cf1", "cf2")
    admin.close()
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
    val admin = HbRddAdmin.apply()
    val splitkeys = List("ooooo", "iiiii")  //split keys
    admin.createTableByProperties("testProperties", this.getPropertiesFamilys, splitkeys)
    admin.close()
  }

  private def deleteTable() = {
    val admin = HbRddAdmin.apply()
    admin.dropTable(tableName)
    admin.close()
  }

  private def truncateTable() = {
    val admin = HbRddAdmin.apply()
    admin.truncateTable(tableName, preserveSplits = true)
    admin.close()
  }

  private def tableSnapshot() = {
    val admin = HbRddAdmin.apply()
    admin.tableSnapshot(tableName)
    admin.close()
  }

  private def testAddFamilies() = {
    val map2 = Map("maxversions" -> "1000", "minversions" -> "2", "ttl" -> "9999", "blocksize" -> "222",
      "inmem" -> "true", "bloomfilter" -> "rowcol", "scope" -> "1", "keepdeletecells" -> "false", "blockcache" -> "false")

    val fp2 = FamilyPropertiesStringSetter(map2)
    val cf2 = HbRddFamily("addcf", fp2)

    val admin = HbRddAdmin.apply()
    admin.addFamilies("testProperties", cf2)
    admin.close()
  }

  private def testUpdateFamilies() = {
    val map2 = Map("maxversions" -> "10001", "minversions" -> "3", "ttl" -> "9990", "blocksize" -> "2212",
      "inmem" -> "false", "bloomfilter" -> "row", "scope" -> "0", "keepdeletecells" -> "true", "blockcache" -> "true")

    val fp2 = FamilyPropertiesStringSetter(map2)
    val cf2 = HbRddFamily("addcf", fp2)

    val admin = HbRddAdmin.apply()
    admin.updateFamilies("testProperties", cf2)
    admin.close()
  }

  private def testDeleteFamilies() = {
    val admin = HbRddAdmin.apply()
    admin.deleteFamiliesByName("testProperties", "addcf")
    admin.close()
  }

  def main(args: Array[String]) {
    println("===")
//    this.tableSnapshot()
//    this.createtable()
//    this.truncateTable()
//    this.deleteTable()

//    this.createtableByProperties()
//    this.testAddFamilies()
//    this.testUpdateFamilies()
//    this.testDeleteFamilies()
  }
}
