package com.audienceproject.crossbow.expr

import com.audienceproject.crossbow.DataFrame
import com.audienceproject.crossbow.exceptions.{AggregationException, InvalidExpressionException}
import com.audienceproject.crossbow.expr.Aggregator.Reducer

/**
 * Aggregators are also Exprs. If evaluated as an Aggregator with reduceOn, it can be used by the GroupBy algorithm.
 * If compiled directly as an Expr, it will throw an AggregationException to indicate unintended use.
 *
 * @param expr the inner Expr
 */
abstract class Aggregator(private val expr: Expr) extends Expr {

  override private[crossbow] def compile(context: DataFrame) = throw new AggregationException(this)

  private[crossbow] def reduceOn(context: DataFrame): Reducer[Any, Any] = {
    val eval = expr.compile(context)
    typeSpec(eval).asInstanceOf[Reducer[Any, Any]]
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

  def unapply(arg: Aggregator): Option[Expr] = Some(arg.expr)

}
