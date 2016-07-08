package top.spoofer.hbrdd.unit

import org.apache.hadoop.hbase.util.Bytes
import org.json4s._

trait HbRddReaders {
  implicit val hbBooleanReader = new HbRddFormatsReader[Boolean] {
    def formatsRead(readData: Array[Byte]): Boolean = Bytes.toBoolean(readData)
  }

  implicit val hbByteArrayReader = new HbRddFormatsReader[Array[Byte]] {
    def formatsRead(readData: Array[Byte]): Array[Byte] = readData
  }

  implicit val hbShortReader = new HbRddFormatsReader[Short] {
    def formatsRead(readData: Array[Byte]): Short = Bytes.toShort(readData)
  }

  implicit val hbIntReader = new HbRddFormatsReader[Int] {
    def formatsRead(readData: Array[Byte]): Int = Bytes.toInt(readData)
  }

  implicit val hbFloatReader = new HbRddFormatsReader[Float] {
    def formatsRead(readData: Array[Byte]): Float = Bytes.toFloat(readData)
  }

  implicit val hbDoubleReader = new HbRddFormatsReader[Double] {
    def formatsRead(readData: Array[Byte]): Double = Bytes.toDouble(readData)
  }

  implicit val hbLongReader = new HbRddFormatsReader[Long] {
    def formatsRead(readData: Array[Byte]): Long = Bytes.toLong(readData)
  }

  implicit val hbStringReader = new HbRddFormatsReader[String] {
    def formatsRead(readData: Array[Byte]): String = Bytes.toString(readData)
  }

  /**
   * 在Bytes.toString(readData)后, 如果字符串的格式不是json, 会出问题！
   */
  implicit val hbJsonReader = new HbRddFormatsReader[JValue] {
    import org.json4s.jackson.JsonMethods._
    def formatsRead(readData: Array[Byte]): JValue = parse(Bytes.toString(readData))
  }
}
