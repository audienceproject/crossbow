package com.audienceproject.crossbow

import com.audienceproject.crossbow.expr.Expr

import scala.reflect.ClassTag

class DataFrame(private val columns: List[Array[_]]) {

  private val schema = columns.indices.map(i => s"_$i")

  def getColumn[T: ClassTag](index: Int): IndexedSeq[T] = {
    // TODO: Typecheck.
    columns(index).asInstanceOf[Array[T]]
  }

  def getColumn[T](columnName: String): IndexedSeq[T] = getColumn(schema.indexWhere(_ == columnName))

  def filter(expr: Expr): DataFrame = {
    val op = expr.compile(this).as[Boolean]
    this
  }

  class View[T] {
  }

}

object DataFrame {

  def main(args: Array[String]): Unit = {
    val df = fromSeq(Seq(("a", 1), ("b", 2), ("c", 3)))
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
