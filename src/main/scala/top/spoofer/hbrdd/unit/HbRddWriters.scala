package top.spoofer.hbrdd.unit

import org.apache.hadoop.hbase.util.Bytes
import org.json4s._

trait HbRddWriters {
  implicit val hbBooleanWriter = new HbRddWriter[Boolean] {
    def write(writeData: Boolean): Array[Byte] = Bytes.toBytes(writeData)
  }

  implicit val hbArrayWriter = new HbRddWriter[Array[Byte]] {
    def write(writeData: Array[Byte]): Array[Byte] = writeData
  }

  implicit val hbShortWriter = new HbRddWriter[Short] {
    def write(writeData: Short): Array[Byte] = Bytes.toBytes(writeData)
  }

  implicit val hbIntWriter = new HbRddWriter[Int] {
    def write(writeData: Int): Array[Byte] = Bytes.toBytes(writeData)
  }

  implicit val hbFloatWriter = new HbRddWriter[Float] {
    def write(writeData: Float): Array[Byte] =  Bytes.toBytes(writeData)
  }

  implicit val hbDoubleWrite = new HbRddWriter[Double] {
    def write(writeData: Double): Array[Byte] = Bytes.toBytes(writeData)
  }

  implicit val hbLongWrite = new HbRddWriter[Long] {
    def write(writeData: Long): Array[Byte] = Bytes.toBytes(writeData)
  }

  implicit val hbStringWrite = new HbRddWriter[String] {
    def write(writeData: String): Array[Byte] = Bytes.toBytes(writeData)
  }

  implicit val hbJsonWrite = new HbRddWriter[JValue] {
    import org.json4s.jackson.JsonMethods._
    def write(writeData: JValue): Array[Byte] = Bytes.toBytes(compact(writeData))
  }
}
