package com.audienceproject.crossbow

import com.audienceproject.crossbow.Implicits._
import com.audienceproject.crossbow.expr.Expr
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{FieldVector, IntVector}

class DataFrame(private val columns: Seq[FieldVector] = Seq.empty) {

  private val allocator = new RootAllocator()

  def filter(expr: Expr): DataFrame = {
    val op = expr.compile().as[Boolean]
    this
  }

  override def finalize(): Unit = {
    super.finalize()
    columns.foreach(_.close())
  }

}

object DataFrame {

  def main(args: Array[String]): Unit = {
    val df = fromSeq(Seq(1, 2, 3))
    df.filter($"int vector" + $"long vector" > 3)
  }

  def fromSeq(seq: Seq[Int]): DataFrame = {
    val col = new IntVector("int vector", new RootAllocator())
    col.allocateNew(seq.size)
    seq.zipWithIndex.foreach({ case (x, i) => col.set(i, x) })
    col.setValueCount(seq.size)
    new DataFrame(Seq(col))
  }

}
