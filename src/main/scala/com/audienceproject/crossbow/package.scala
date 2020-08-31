package com.audienceproject

import com.audienceproject.crossbow.expr._

import scala.reflect.ClassTag

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

  private[crossbow] def sliceColumn(eval: Specialized[_], indices: IndexedSeq[Int]): Array[_] = {
    eval.typeOf match {
      case IntType => fillArray[Int](indices, eval.as[Int].apply)
      case LongType => fillArray[Long](indices, eval.as[Long].apply)
      case DoubleType => fillArray[Double](indices, eval.as[Double].apply)
      case BooleanType => fillArray[Boolean](indices, eval.as[Boolean].apply)
      case _ => fillArray[Any](indices, eval.apply)
    }
  }

  private[crossbow] def padColumn(data: Array[_], ofType: Type, padding: Int): Array[_] = {
    ofType match {
      case IntType => fillArray[Int](data.indices, data.asInstanceOf[Array[Int]], padding)
      case LongType => fillArray[Long](data.indices, data.asInstanceOf[Array[Long]], padding)
      case DoubleType => fillArray[Double](data.indices, data.asInstanceOf[Array[Double]], padding)
      case BooleanType => fillArray[Boolean](data.indices, data.asInstanceOf[Array[Boolean]], padding)
      case _ => fillArray[Any](data.indices, data.asInstanceOf[Array[Any]], padding)
    }
  }

  private[crossbow] def fillArray[T: ClassTag](indices: IndexedSeq[Int], getValue: Int => T, padding: Int = 0): Array[T] = {
    val arr = new Array[T](indices.size + math.abs(padding))
    val indexOffset = math.max(padding, 0)
    for (i <- indices.indices if indices(i) >= 0) arr(i + indexOffset) = getValue(indices(i))
    arr
  }

  private[crossbow] def spliceColumns(data: Seq[Array[_]], ofType: Type): Array[_] = {
    ofType match {
      case IntType => fillNArray[Int](data.map(_.asInstanceOf[Array[Int]]))
      case LongType => fillNArray[Long](data.map(_.asInstanceOf[Array[Long]]))
      case DoubleType => fillNArray[Double](data.map(_.asInstanceOf[Array[Double]]))
      case BooleanType => fillNArray[Boolean](data.map(_.asInstanceOf[Array[Boolean]]))
      case _ => fillNArray[Any](data.map(_.asInstanceOf[Array[Any]]))
    }
  }

  private[crossbow] def fillNArray[T: ClassTag](nData: Seq[Array[T]]): Array[T] = {
    val arr = new Array[T](nData.map(_.length).sum)
    nData.foldLeft(0) {
      case (offset, data) =>
        for (i <- data.indices) arr(i + offset) = data(i)
        data.length + offset
    }
    arr
  }

  private[crossbow] def convert(data: Seq[Any], dataType: Type): Array[_] = {
    dataType match {
      case IntType => data.asInstanceOf[Seq[Int]].toArray
      case LongType => data.asInstanceOf[Seq[Long]].toArray
      case DoubleType => data.asInstanceOf[Seq[Double]].toArray
      case BooleanType => data.asInstanceOf[Seq[Boolean]].toArray
      case _ => data.toArray
    }
  }

}
