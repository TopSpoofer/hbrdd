package top.spoofer.hbrdd.unit

import org.apache.hadoop.hbase.util.Bytes
import org.json4s._

trait HbRddReaders {
  implicit val hbBooleanReader = new HbRddReader[Boolean] {
    def read(readData: Array[Byte]): Boolean = Bytes.toBoolean(readData)
  }

  implicit val hbByteArrayReader = new HbRddReader[Array[Byte]] {
    def read(readData: Array[Byte]): Array[Byte] = readData
  }

  implicit val hbShortReader = new HbRddReader[Short] {
    def read(readData: Array[Byte]): Short = Bytes.toShort(readData)
  }

  implicit val hbIntReader = new HbRddReader[Int] {
    def read(readData: Array[Byte]): Int = Bytes.toInt(readData)
  }

  implicit val hbFloatReader = new HbRddReader[Float] {
    def read(readData: Array[Byte]): Float = Bytes.toFloat(readData)
  }

  implicit val hbDoubleReader = new HbRddReader[Double] {
    def read(readData: Array[Byte]): Double = Bytes.toDouble(readData)
  }

  implicit val hbLongReader = new HbRddReader[Long] {
    def read(readData: Array[Byte]): Long = Bytes.toLong(readData)
  }

  implicit val hbStringReader = new HbRddReader[String] {
    def read(readData: Array[Byte]): String = Bytes.toString(readData)
  }

  /**
    * 在Bytes.toString(readData)后, 如果字符串的格式不是json, 会出问题！
    */
  implicit val hbJsonReader = new HbRddReader[JValue] {
    import org.json4s.jackson.JsonMethods._
    def read(readData: Array[Byte]): JValue = parse(Bytes.toString(readData))
  }
}
