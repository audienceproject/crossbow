package com.audienceproject.crossbow.expr

import com.audienceproject.crossbow.exceptions.InvalidExpressionException
import com.audienceproject.crossbow.expr.Aggregator.Reducer

trait AggregationOps {

  self: Expr =>

  def sum(): Aggregator = new Aggregator(this) {
    override protected def typeSpec(op: Specialized[_]): Reducer[_, _] = op.typeOf match {
      case IntType => Reducer[Int, Int](op.as, _ + _, 0, IntType)
      case LongType => Reducer[Long, Long](op.as, _ + _, 0L, LongType)
      case DoubleType => Reducer[Double, Double](op.as, _ + _, 0d, DoubleType)
      case t => throw new InvalidExpressionException("Sum", t)
    }
  }

  def count(): Aggregator = new Aggregator(this) {
    override protected def typeSpec(op: Specialized[_]): Reducer[_, _] =
      Reducer[Any, Int](op, (_, x) => x + 1, 0, IntType)
  }

  def collect(): Aggregator = new Aggregator(this) {
    override protected def typeSpec(op: Specialized[_]): Reducer[_, _] =
      Reducer[Any, Seq[Any]](op, _ +: _, Seq.empty, ListType(op.typeOf))
  }

  def agg[T: ru.TypeTag, U: ru.TypeTag](f: (T, U) => U, seed: U): Aggregator = new Aggregator(this) {
    override protected def typeSpec(op: Specialized[_]): Reducer[_, _] = {
      val spec = op.typecheckAs[T]
      Reducer[T, U](spec, f, seed, Types.toInternalType(ru.typeOf[U]))
    }
  }

}
