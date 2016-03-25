
import java.io._
import scala.util.Random

object TestCreateDatas {
  def printToFile(path: String)(op: PrintWriter => Unit) {
    val p = new PrintWriter(new File(path))
    try { op(p) } finally { p.close() }
  }

  def nextString = (1 to 10) map (_ => Random.nextPrintableChar) mkString
  def nextLine = (1 to 4) map (_ => nextString) mkString "\t"

  def main(args: Array[String]) {
    printToFile(args(0)) { p =>
      for (i <- 1 to 10000000) {
//        if (i == 10000000) println("1kw")
//        if (i == 50000000) println("5kw")
//        if (i == 80000000) println("8kw")
        p.println(nextLine)
      }
    }
  }
}
