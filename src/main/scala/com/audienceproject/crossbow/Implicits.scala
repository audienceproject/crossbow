package com.audienceproject.crossbow

import com.audienceproject.crossbow.exceptions.InvalidExpressionException
import com.audienceproject.crossbow.expr.*

import scala.language.implicitConversions

object Implicits {

  import expr.typeTag

  // Column expression.
  extension (sc: StringContext)
    def $(args: Any*): DataFrame ?=> Expr = Expr.Cell(sc.s(args: _*))

  // Literal value.
  implicit def lit[T](value: T): Expr = Expr.Literal(value)

  // Index function.
  def index(): DataFrame ?=> Expr = Expr.Index()

  // Lambda function.
  def lambda[T, R](f: T => R): Expr => Expr =
    (expr: Expr) => Expr.Unary(expr, f)

  // Sequence of values.
  def seq(exprs: Expr*): Expr = Expr.List(exprs)

  // Aggregators.
  def sum(expr: Expr): Expr = expr.typeOf match
    case RuntimeType.Int => Expr.Aggregate[Int, Int](expr, _ + _, 0)
    case RuntimeType.Long => Expr.Aggregate[Long, Long](expr, _ + _, 0L)
    case RuntimeType.Double => Expr.Aggregate[Double, Double](expr, _ + _, 0d)
    case _ => throw new InvalidExpressionException("sum", expr)

  def count(): Expr = Expr.Aggregate[Any, Int](lit(1), (_, x) => x + 1, 0)

  def collect(expr: Expr): Expr = Expr.Aggregate[Any, Seq[Any]](expr, (e, seq) => seq :+ e, Vector.empty)

  def one(expr: Expr): Expr = Expr.Aggregate[Any, Any](expr, (elem, _) => elem, null)

  // Custom aggregator.
  def reducer[T, U](seed: U)(f: (T, U) => U): Expr => Expr = Expr.Aggregate[T, U](_, f, seed)
}
