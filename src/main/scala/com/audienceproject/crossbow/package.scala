package com.audienceproject

package object crossbow {

  private[crossbow] def toTuple[T](n: Int): Seq[Any] => T = n match {
    case 1 => tuple1
    case 2 => tuple2
    case 3 => tuple3
    case 4 => tuple4
    case 5 => tuple5
    case 6 => tuple6
  }

  private def tuple1[T]: Seq[Any] => T = {
    case Seq(_1) => _1.asInstanceOf[T]
  }

  private def tuple2[T]: Seq[Any] => T = {
    case Seq(_1, _2) => (_1, _2).asInstanceOf[T]
  }

  private def tuple3[T]: Seq[Any] => T = {
    case Seq(_1, _2, _3) => (_1, _2, _3).asInstanceOf[T]
  }

  private def tuple4[T]: Seq[Any] => T = {
    case Seq(_1, _2, _3, _4) => (_1, _2, _3, _4).asInstanceOf[T]
  }

  private def tuple5[T]: Seq[Any] => T = {
    case Seq(_1, _2, _3, _4, _5) => (_1, _2, _3, _4, _5).asInstanceOf[T]
  }

  private def tuple6[T]: Seq[Any] => T = {
    case Seq(_1, _2, _3, _4, _5, _6) => (_1, _2, _3, _4, _5, _6).asInstanceOf[T]
  }

}
