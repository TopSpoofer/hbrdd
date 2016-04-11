import top.spoofer.hbrdd.hbsupport.{HbRddFamily, FamilyPropertiesStringSetter}

object TestHbRddFamily {
  def main(args: Array[String]) {
    val map = Map("maxversions" -> "1000", "ttl" -> "9999", "inmem" -> "true")
    val hh = FamilyPropertiesStringSetter(map)
    println(hh.blocksize)
    println(hh.maxversions)
    println(hh.ttl)
    println(hh.inmem)

    val tt = HbRddFamily("ffff", hh)
    println(tt.getMaxVersions)
  }
}
