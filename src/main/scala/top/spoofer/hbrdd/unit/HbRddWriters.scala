package top.spoofer.hbrdd.unit

import org.apache.hadoop.hbase.util.Bytes
import org.json4s._

trait HbRddWriters {
  implicit val hbBooleanWriter = new HbRddFormatsWriter[Boolean] {
    def formatsWrite(writeData: Boolean): Array[Byte] = Bytes.toBytes(writeData)
  }

  implicit val hbArrayWriter = new HbRddFormatsWriter[Array[Byte]] {
    def formatsWrite(writeData: Array[Byte]): Array[Byte] = writeData
  }

  implicit val hbShortWriter = new HbRddFormatsWriter[Short] {
    def formatsWrite(writeData: Short): Array[Byte] = Bytes.toBytes(writeData)
  }

  implicit val hbIntWriter = new HbRddFormatsWriter[Int] {
    def formatsWrite(writeData: Int): Array[Byte] = Bytes.toBytes(writeData)
  }

  implicit val hbFloatWriter = new HbRddFormatsWriter[Float] {
    def formatsWrite(writeData: Float): Array[Byte] = Bytes.toBytes(writeData)
  }

  implicit val hbDoubleWrite = new HbRddFormatsWriter[Double] {
    def formatsWrite(writeData: Double): Array[Byte] = Bytes.toBytes(writeData)
  }

  implicit val hbLongWrite = new HbRddFormatsWriter[Long] {
    def formatsWrite(writeData: Long): Array[Byte] = Bytes.toBytes(writeData)
  }

  implicit val hbStringWrite = new HbRddFormatsWriter[String] {
    def formatsWrite(writeData: String): Array[Byte] = Bytes.toBytes(writeData)
  }

  implicit val hbJsonWrite = new HbRddFormatsWriter[JValue] {
    import org.json4s.jackson.JsonMethods._
    def formatsWrite(writeData: JValue): Array[Byte] = Bytes.toBytes(compact(writeData))
  }
}
