package com.audienceproject.crossbow

import com.audienceproject.crossbow.expr.Expr

import scala.collection.immutable.ArraySeq
import scala.reflect.ClassTag

class DataFrame(private val columns: List[Array[_]]) extends Iterable[Seq[Any]] {

  private val schema = columns.indices.map(i => s"_$i")

  val rowCount: Int = columns.head.length
  val numColumns: Int = columns.size

  def apply(index: Int): Seq[Any] = columns.map(_ (index))

  def apply(range: Range): DataFrame = ???

  def as[T <: Product]: TypedView[T] = {
    // TODO: Typecheck.
    new TypedView[T]
  }

  def filter(expr: Expr): DataFrame = {
    val op = expr.compile(this).as[Boolean]
    val indices = for (i <- 0 until rowCount if op(i)) yield i
    val newCols = List.fill(numColumns)(new Array[Any](indices.size))
    for ((oldIndex, newIndex) <- indices.zipWithIndex)
      for (c <- columns.indices)
        newCols(c)(newIndex) = columns(c)(oldIndex)
    new DataFrame(newCols)
  }

  private[crossbow] def getColumn(columnName: String): Array[_] = {
    val columnIndex = schema.indexWhere(_ == columnName)
    columns(columnIndex)
  }

  class TypedView[T <: Product] extends Iterable[T] {
    private implicit val t2Tuple: Seq[Any] => T = toTuple[T](numColumns)

    def apply(index: Int): T = DataFrame.this (index)

    def apply(range: Range): Seq[T] = for (i <- range) yield this (i)

    override def iterator: Iterator[T] = this (0 until rowCount).iterator
  }

  override def iterator: Iterator[Seq[Any]] = (for (i <- 0 until rowCount) yield this (i)).iterator

}

object DataFrame {

  def main(args: Array[String]): Unit = {
    val df = fromSeq(Seq(("a", 1), ("b", 2), ("c", 3)))
    df.as[(String, Int)].foreach({
      case (s, x) => println(s"$s: ${s.getClass}; $x: ${x.getClass}")
    })
  }

  def fromSeq[T <: Product](data: Seq[T]): DataFrame = {
    if (data.isEmpty) new DataFrame(List.empty)
    else {
      val first = data.head
      val columns = List.fill(first.productArity)(Array.newBuilder[Any])
      for (d <- data)
        for (i <- 0 until d.productArity)
          columns(i) += d.productElement(i)
      new DataFrame(columns.map(_.result()))
    }
  }

}
