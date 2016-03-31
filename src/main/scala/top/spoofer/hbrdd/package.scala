package top.spoofer

import top.spoofer.hbrdd.hbsupport.{HbRddDeleter, HbRddReader, HbRddWriter, HbRddManager}
import top.spoofer.hbrdd.unit.{basalImpl, HbRddReaders, HbRddWriters}


package object hbrdd extends HbRddWriter
  with HbRddWriters
  with HbRddReader
  with HbRddReaders
  with HbRddManager
  with HbRddDeleter
  with basalImpl

