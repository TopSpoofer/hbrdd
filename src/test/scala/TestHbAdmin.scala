
import top.spoofer.hbrdd._
import top.spoofer.hbrdd.config.HbRddConfig

object TestHbAdmin {
  val tableName = "test_hbrdd"
  implicit val hbConfig = HbRddConfig()

  private def createtable() = {
    val hh = HbRddAdmin.apply()
    val l = List("ooooo", "iiiii")
    hh.createTable(tableName, l, "cf1", "cf2")
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
    this.createtable()
//    this.truncateTable()
//    this.deleteTable()
  }
}
