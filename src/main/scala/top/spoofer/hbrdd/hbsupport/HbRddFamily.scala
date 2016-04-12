package top.spoofer.hbrdd.hbsupport

import org.apache.hadoop.hbase.{KeepDeletedCells, HColumnDescriptor}
import org.apache.hadoop.hbase.regionserver.BloomType


sealed trait FamilyProperties {
  protected var _maxversions: Int = 1
  protected var _minversions: Int = 0
  protected var _ttl: Int = 2147483647
  protected var _blocksize: Int = 65536
  protected var _inmem: Boolean = false
  protected var _bloomfilter: BloomType = BloomType.ROW
  protected var _scope: Int = 0
  protected var _kdc: KeepDeletedCells = KeepDeletedCells.FALSE
  protected var _blockcache: Boolean = true

  def maxversions: Int = this._maxversions
  def minversions: Int = this._minversions
  def ttl: Int = this._ttl
  def blocksize: Int = this._blocksize
  def inmem: Boolean = this._inmem
  def bloomfilter: BloomType = this._bloomfilter
  def scope: Int = this._scope
  def keepdeletecells: KeepDeletedCells = this._kdc
  def blockcache: Boolean = this._blockcache
  //暂时不支持Compression

  def setHColumnDescriptorAttrs(hbRddFamily: HbRddFamily)
}

final class FamilyPropertiesStringSetter() extends FamilyProperties {

  def this(properties: Map[String, String]) = {
    this()
    this.initProperties(properties)
  }

  /**
    * 如果attr的值不是true或者false的话，默认返回false
    * @param attr 需要转换的属性
    * @return
    */
  implicit private def stringToBoolean(attr: String): Boolean = {
    attr match {
      case "true" => true
      case "false" => false
      case _ => false
    }
  }

  /**
    * 如果filterStr不是row或者rowcol的话,默认为NONE
    * @param filterStr 过滤器名称
    * @return
    */
  implicit private def stringToBloomFilter(filterStr: String): BloomType = {
    filterStr match {
      case "row" => BloomType.ROW
      case "rowcol" => BloomType.ROWCOL
      case _ => BloomType.NONE
    }
  }

  /**
    * 如果value不是true、false、ttl的话,默认为false
    * @param kdcStr KeepDeletedCells
    * @return
    */
  implicit private def stringToKeepDeletedCells(kdcStr: String): KeepDeletedCells = {
    kdcStr match {
      case "true" => KeepDeletedCells.TRUE
      case "false" => KeepDeletedCells.FALSE
      case "ttl" => KeepDeletedCells.TTL
      case _ => KeepDeletedCells.FALSE
    }
  }

  private def setHColumnDescriptorAttrsStr(attrName: String, attrValue: String) = {
    attrName match {
      case "maxversions" => this._maxversions = attrValue.toInt
      case "minversions" => this._minversions = attrValue.toInt
      case "ttl" => this._ttl = attrValue.toInt
      case "blocksize" => this._blocksize = attrValue.toInt
      case "inmem" => this._inmem = attrValue
      case "bloomfilter" => this._bloomfilter = attrValue
      case "scope" => this._scope = attrValue.toInt
      case "keepdeletecells" => this._kdc = attrValue
      case "blockcache" => this._blockcache = attrValue //**********
      case _ =>
    }
  }

  private def initProperties(attrs: Map[String, String]) = {
    attrs.keys foreach { attrName =>
      this.setHColumnDescriptorAttrsStr(attrName, attrs(attrName))
    }
  }

  def setHColumnDescriptorAttrs(hbRddFamily: HbRddFamily) = {
    hbRddFamily.setMaxVersions(this.maxversions)
    hbRddFamily.setMinVersions(this.minversions)
    hbRddFamily.setTimeToLive(this.ttl)
    hbRddFamily.setBlocksize(this.blocksize)
    hbRddFamily.setInMemory(this.inmem)
    hbRddFamily.setBloomFilterType(this.bloomfilter)
    hbRddFamily.setScope(this.scope)
    hbRddFamily.setKeepDeletedCells(this.keepdeletecells)
    hbRddFamily.setBlockCacheEnabled(this.blockcache)
  }
}


final class FamilyPropertiesSetter() extends FamilyProperties {
  def this(properties: Map[String, Any]) = {
    this()
    this.initProperties(properties)
  }

  private def doSetHColumnDescriptorAttrs[A <: Any](attrName: String, attrValue: A) = {
    attrName match {
      case "maxversions" => this._maxversions = attrValue.asInstanceOf[Int]
      case "minversions" => this._minversions = attrValue.asInstanceOf[Int]
      case "ttl" => this._ttl = attrValue.asInstanceOf[Int]
      case "blocksize" => this._blocksize = attrValue.asInstanceOf[Int]
      case "inmem" => this._inmem = attrValue.asInstanceOf[Boolean]
      case "bloomfilter" => this._bloomfilter = attrValue.asInstanceOf[BloomType]
      case "scope" => this._scope = attrValue.asInstanceOf[Int]
      case "keepdeletecells" => this._kdc = attrValue.asInstanceOf[KeepDeletedCells]
      case "blockcache" => this._blockcache = attrValue.asInstanceOf[Boolean]
      case _ =>
    }
  }

  private def initProperties[A <: Any](attrs: Map[String, A]) = {
    attrs.keys foreach { attrName =>
      this.doSetHColumnDescriptorAttrs(attrName, attrs(attrName))
    }
  }

  def setHColumnDescriptorAttrs(hbRddFamily: HbRddFamily) = {
    hbRddFamily.setMaxVersions(this.maxversions)
    hbRddFamily.setMinVersions(this.minversions)
    hbRddFamily.setTimeToLive(this.ttl)
    hbRddFamily.setBlocksize(this.blocksize)
    hbRddFamily.setInMemory(this.inmem)
    hbRddFamily.setBloomFilterType(this.bloomfilter)
    hbRddFamily.setScope(this.scope)
    hbRddFamily.setKeepDeletedCells(KeepDeletedCells.valueOf("true"))
    hbRddFamily.setBlockCacheEnabled(this.blockcache)
  }
}

object FamilyPropertiesStringSetter {
  def apply(): FamilyPropertiesStringSetter = {
    new FamilyPropertiesStringSetter()
  }

  def apply(properties: Map[String, String]): FamilyPropertiesStringSetter = {
    new FamilyPropertiesStringSetter(properties)
  }
}

object FamilyPropertiesSetter {
  def apply(): FamilyPropertiesSetter = {
    new FamilyPropertiesSetter()
  }

  def apply(properties: Map[String, Any]): FamilyPropertiesSetter = {
    new FamilyPropertiesSetter(properties)
  }
}


class HbRddFamily(family: String) extends HColumnDescriptor(family) {
  def this(family: String, familyProperties: FamilyProperties) = {
    this(family)
    familyProperties.setHColumnDescriptorAttrs(this)
  }

  def name: String = this.getNameAsString
}

object HbRddFamily {
  def apply(family: String): HbRddFamily = {
    new HbRddFamily(family)
  }

  def apply(family: String, familyProperties: FamilyProperties): HbRddFamily = {
    new HbRddFamily(family, familyProperties)
  }
}