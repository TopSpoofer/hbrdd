package top.spoofer.hbrdd.unit

import org.apache.hadoop.hbase.util.Bytes

trait basalImpl {
  implicit def arrayStringToArrayBytes(array: Array[String]): Array[Array[Byte]]  = array map { Bytes.toBytes }
  implicit def stringToBytes(str: String): Array[Byte] = Bytes.toBytes(str)
}
