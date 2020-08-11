package com.audienceproject.crossbow

import com.audienceproject.crossbow.expr.Expr
import com.audienceproject.crossbow.expr.Expr.Column
import com.audienceproject.crossbow.schema.Schema

class DataFrame private(private val columns: List[Array[_]],
                        val schema: Schema) extends Iterable[Seq[Any]] {

  val rowCount: Int = columns.head.length
  val numColumns: Int = columns.size

  def apply(index: Int): Seq[Any] = columns.map(_ (index))

  def apply(range: Range): DataFrame = slice(range)

  def apply(columnNames: String*): DataFrame = {
    val colExprs = columnNames.map(Expr.Column)
    select(colExprs: _*)
  }

  def as[T]: TypedView[T] = {
    // TODO: Typecheck.
    new TypedView[T]
  }

  def select(exprs: Expr*): DataFrame = {
    val cols = exprs.map({
      case Column(columnName) => getColumn(columnName)
      case expr =>
        val op = expr.compile(this)
        Array.from((0 until rowCount) map op.apply)
    })
    new DataFrame(cols.toList, schema) // TODO: Schema.
  }

  def filter(expr: Expr): DataFrame = {
    val op = expr.compile(this).as[Boolean]
    val indices = for (i <- 0 until rowCount if op(i)) yield i
    slice(indices)
  }

  private def slice(indices: IndexedSeq[Int]): DataFrame = {
    val newCols = List.fill(numColumns)(new Array[Any](indices.size))
    for ((oldIndex, newIndex) <- indices.zipWithIndex)
      for (c <- columns.indices)
        newCols(c)(newIndex) = columns(c)(oldIndex)
    new DataFrame(newCols, schema)
  }

  private[crossbow] def getColumn(columnName: String): Array[_] = {
    val columnIndex = schema.indexOf(columnName)
    columns(columnIndex)
  }

  class TypedView[T] extends Iterable[T] {
    private implicit val t2Tuple: Seq[Any] => T = toTuple[T](numColumns)

    def apply(index: Int): T = DataFrame.this (index)

    def apply(range: Range): Seq[T] = for (i <- range) yield this (i)

    override def iterator: Iterator[T] = this (0 until rowCount).iterator
  }

  override def iterator: Iterator[Seq[Any]] = (for (i <- 0 until rowCount) yield this (i)).iterator

}

object DataFrame {

  def main(args: Array[String]): Unit = {
    import Implicits._
    val df = fromSeq(Seq(("a", 1), ("b", 2), ("c", 3)))
    val toStringFunction = lambda[Int, String] {
      _.toString
    }
    df.select(toStringFunction($"hej"))
    df.as[(String, Int)].foreach({
      case (s, x) => println(s"$s: ${s.getClass}; $x: ${x.getClass}")
    })
  }

  def fromSeq[T](data: Seq[T]): DataFrame = {
    if (data.isEmpty) new DataFrame(List.empty, Schema())
    else {
      data.head match {
        case p: Product =>
          val columns = List.fill(p.productArity)(Array.newBuilder[Any])
          for (d <- data.asInstanceOf[Seq[Product]])
            for (i <- 0 until d.productArity)
              columns(i) += d.productElement(i)
          new DataFrame(columns.map(_.result()), Schema()) // TODO: Schema.
        case _: Any =>
          val col = Array.newBuilder[Any]
          for (d <- data) col += d
          new DataFrame(List(col.result()), Schema()) // TODO: Schema.
      }
    }
  }

}
