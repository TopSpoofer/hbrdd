package top.spoofer.hbrdd.unit

/**
  * HbRddReader 提供了将Array[Byte]转换为A类型的接口
  * @tparam A 类型参数
  */
trait HbRddFormatsReader[A] extends Serializable {
  def formatsRead(readData: Array[Byte]): A
}

/**
  * HbRddWriter 提供了将 A类型转化为Array[Byte]的接口
  * @tparam A 类型参数
  */
trait HbRddFormatsWriter[A] extends Serializable {
  def formatsWrite(writeData: A): Array[Byte]
}

trait HbRddFormats[A] extends HbRddFormatsReader[A] with HbRddFormatsWriter[A]
