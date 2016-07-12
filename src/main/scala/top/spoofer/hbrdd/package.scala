package top.spoofer

import top.spoofer.hbrdd.hbsupport._
import top.spoofer.hbrdd.unit.{HbRddReaders, HbRddWriters, basalImpl}

package object hbrdd extends HbRddWriter
  with HbRddWriters
  with HbRddBulker
  with HbRddReader
  with HbRddReaders
  with HbRddManager
  with HbRddDeleter
  with basalImpl