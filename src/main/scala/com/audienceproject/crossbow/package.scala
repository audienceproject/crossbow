package com.audienceproject

package object crossbow {

  private[crossbow] def toTuple[T <: Product](n: Int): Seq[Any] => T = n match {
    case 1 => tuple1
    case 2 => tuple2
    case 3 => tuple3
    case 4 => tuple4
  }

  private def tuple1[T <: Product]: Seq[Any] => T = {
    case Seq(_1) => Tuple1(_1).asInstanceOf[T]
  }

  private def tuple2[T <: Product]: Seq[Any] => T = {
    case Seq(_1, _2) => Tuple2(_1, _2).asInstanceOf[T]
  }

  private def tuple3[T <: Product]: Seq[Any] => T = {
    case Seq(_1, _2, _3) => Tuple1(_1, _2, _3).asInstanceOf[T]
  }

  private def tuple4[T <: Product]: Seq[Any] => T = {
    case Seq(_1, _2, _3, _4) => Tuple1(_1, _2, _3, _4).asInstanceOf[T]
  }

  private def tuple5[T <: Product]: Seq[Any] => T = {
    case Seq(_1, _2, _3, _4, _5) => Tuple1(_1, _2, _3, _4, _5).asInstanceOf[T]
  }

  private def tuple6[T <: Product]: Seq[Any] => T = {
    case Seq(_1, _2, _3, _4, _5, _6) => Tuple1(_1, _2, _3, _4, _5, _6).asInstanceOf[T]
  }

}
