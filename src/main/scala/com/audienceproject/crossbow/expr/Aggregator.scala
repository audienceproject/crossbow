package com.audienceproject.crossbow.expr

import com.audienceproject.crossbow.DataFrame
import com.audienceproject.crossbow.exceptions.InvalidExpressionException
import com.audienceproject.crossbow.expr.Aggregator.Reducer

/**
 * Aggregators are also Exprs. If evaluated as an Aggregator with reduceOn, it can be used by the GroupBy algorithm.
 * If compiled directly as an Expr, it evaluates to the reduction applied exactly once to the inner Expr and the seed.
 *
 * @param expr the inner Expr
 */
abstract class Aggregator(expr: Expr) extends Expr {

  override private[crossbow] def compile(context: DataFrame) = {
    // TODO: Consider throwing an exception instead of accepting Aggregators in a non-reducing context.
    val op = expr.compile(context)
    val reducer = reduceOn(context)
    new Specialized[Any] {
      override def apply(i: Int): Any = reducer.f(op(i), reducer.seed)

      override val typeOf: Type = reducer.typeOf
    }
  }

  private[crossbow] def reduceOn(context: DataFrame): Reducer[Any, Any] = {
    val op = expr.compile(context)
    typeSpec(op).asInstanceOf[Reducer[Any, Any]]
  }

  protected def typeSpec(op: Specialized[_]): Reducer[_, _]

}

private[crossbow] object Aggregator {

  case class Reducer[T, U](specialized: Specialized[T], f: (T, U) => U, seed: U, typeOf: Type) {
    def apply(i: Int, agg: U): U = f(specialized(i), agg)
  }

  case class Sum(expr: Expr) extends Aggregator(expr) {
    override protected def typeSpec(op: Specialized[_]): Reducer[_, _] = op.typeOf match {
      case IntType => Reducer[Int, Int](op.as, _ + _, 0, IntType)
      case LongType => Reducer[Long, Long](op.as, _ + _, 0L, LongType)
      case DoubleType => Reducer[Double, Double](op.as, _ + _, 0d, DoubleType)
      case t => throw new InvalidExpressionException("Sum", t)
    }
  }

  case class Count(expr: Expr) extends Aggregator(expr) {
    override protected def typeSpec(op: Specialized[_]): Reducer[_, _] =
      Reducer[Any, Int](op, (_, x) => x + 1, 0, IntType)
  }

  case class Collect(expr: Expr) extends Aggregator(expr) {
    override protected def typeSpec(op: Specialized[_]): Reducer[_, _] =
      Reducer[Any, Seq[Any]](op, (e, seq) => seq :+ e, Vector.empty, ListType(op.typeOf))
  }

  case class OneOf(expr: Expr) extends Aggregator(expr) {
    override protected def typeSpec(op: Specialized[_]): Reducer[_, _] =
      Reducer[Any, Any](op, (elem, _) => elem, null, op.typeOf)
  }

}
