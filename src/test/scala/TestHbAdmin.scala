
import top.spoofer.hbrdd._
import top.spoofer.hbrdd.config.HbRddConfig

object TestHbAdmin {
  def main(args: Array[String]) {
    val tableName = "test_hbrdd"
    implicit val hbConfig = HbRddConfig()
    val hh = HbRddAdmin.apply()
    val l = List("ooooo", "iiiii")
//    println(hh.tableExists(tableName, "cf3"))
    hh.createTable(tableName, l, "cf1", "cf2")
    hh.close()
  }
}
