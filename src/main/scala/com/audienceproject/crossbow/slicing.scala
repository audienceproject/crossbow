package com.audienceproject.crossbow

import com.audienceproject.crossbow.expr.RuntimeType

import scala.reflect.ClassTag

private def sliceColumn(eval: Int => ?, ofType: RuntimeType, indices: IndexedSeq[Int]): Array[?] =
  ofType match
    case RuntimeType.Int => fillArray[Int](indices, eval.asInstanceOf[Int => Int])
    case RuntimeType.Long => fillArray[Long](indices, eval.asInstanceOf[Int => Long])
    case RuntimeType.Double => fillArray[Double](indices, eval.asInstanceOf[Int => Double])
    case RuntimeType.Float => fillArray[Float](indices, eval.asInstanceOf[Int => Float])
    case RuntimeType.Boolean => fillArray[Boolean](indices, eval.asInstanceOf[Int => Boolean])
    case _ => fillArray[Any](indices, eval)

private def padColumn(data: Array[?], ofType: RuntimeType, padding: Int): Array[?] =
  ofType match
    case RuntimeType.Int => fillArray[Int](data.indices, data.asInstanceOf[Array[Int]], padding)
    case RuntimeType.Long => fillArray[Long](data.indices, data.asInstanceOf[Array[Long]], padding)
    case RuntimeType.Double => fillArray[Double](data.indices, data.asInstanceOf[Array[Double]], padding)
    case RuntimeType.Float => fillArray[Float](data.indices, data.asInstanceOf[Array[Float]], padding)
    case RuntimeType.Boolean => fillArray[Boolean](data.indices, data.asInstanceOf[Array[Boolean]], padding)
    case _ => fillArray[Any](data.indices, data.asInstanceOf[Array[Any]], padding)

private def fillArray[T: ClassTag](indices: IndexedSeq[Int], getValue: Int => T, padding: Int = 0): Array[T] =
  val arr = new Array[T](indices.size + math.abs(padding))
  val indexOffset = math.max(padding, 0)
  for (i <- indices.indices if indices(i) >= 0) arr(i + indexOffset) = getValue(indices(i))
  arr

private def spliceColumns(data: Seq[Array[?]], ofType: RuntimeType): Array[?] =
  ofType match
    case RuntimeType.Int => fillNArray[Int](data.map(_.asInstanceOf[Array[Int]]))
    case RuntimeType.Long => fillNArray[Long](data.map(_.asInstanceOf[Array[Long]]))
    case RuntimeType.Double => fillNArray[Double](data.map(_.asInstanceOf[Array[Double]]))
    case RuntimeType.Float => fillNArray[Float](data.map(_.asInstanceOf[Array[Float]]))
    case RuntimeType.Boolean => fillNArray[Boolean](data.map(_.asInstanceOf[Array[Boolean]]))
    case _ => fillNArray[Any](data.map(_.asInstanceOf[Array[Any]]))

private def fillNArray[T: ClassTag](nData: Seq[Array[T]]): Array[T] =
  val arr = new Array[T](nData.map(_.length).sum)
  nData.foldLeft(0):
    case (offset, data) =>
      for (i <- data.indices) arr(i + offset) = data(i)
      data.length + offset
  arr

private def repeatColumn(data: Array[?], ofType: RuntimeType, reps: Array[Int]): Array[?] =
  ofType match
    case RuntimeType.Int => fillRepeatArray[Int](data.asInstanceOf[Array[Int]], reps)
    case RuntimeType.Long => fillRepeatArray[Long](data.asInstanceOf[Array[Long]], reps)
    case RuntimeType.Double => fillRepeatArray[Double](data.asInstanceOf[Array[Double]], reps)
    case RuntimeType.Float => fillRepeatArray[Float](data.asInstanceOf[Array[Float]], reps)
    case RuntimeType.Boolean => fillRepeatArray[Boolean](data.asInstanceOf[Array[Boolean]], reps)
    case _ => fillRepeatArray[Any](data.asInstanceOf[Array[Any]], reps)

private def fillRepeatArray[T: ClassTag](data: Array[T], reps: Array[Int]): Array[T] =
  val arr = new Array[T](reps.sum)
  reps.indices.foldLeft(0):
    case (offset, i) =>
      val next = offset + reps(i)
      for (j <- offset until next) arr(j) = data(i)
      next
  arr

private def convert(data: Seq[Any], dataType: RuntimeType): Array[?] =
  dataType match
    case RuntimeType.Int => data.asInstanceOf[Seq[Int]].toArray
    case RuntimeType.Long => data.asInstanceOf[Seq[Long]].toArray
    case RuntimeType.Double => data.asInstanceOf[Seq[Double]].toArray
    case RuntimeType.Float => data.asInstanceOf[Seq[Float]].toArray
    case RuntimeType.Boolean => data.asInstanceOf[Seq[Boolean]].toArray
    case _ => data.toArray
