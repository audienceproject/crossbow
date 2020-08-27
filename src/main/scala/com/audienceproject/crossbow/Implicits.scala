package com.audienceproject.crossbow

import com.audienceproject.crossbow.expr.Aggregator.Reducer
import com.audienceproject.crossbow.expr._

import scala.language.implicitConversions

object Implicits {

  // Column expression.
  implicit class ColumnByName(val sc: StringContext) extends AnyVal {
    def $(args: Any*): Expr = Expr.Cell(sc.s(args: _*))
  }

  // Literal value.
  implicit def lit[T: ru.TypeTag](value: T): Expr = Expr.Literal(value)

  // Tuples.
  implicit def tuple2(t: (Expr, Expr)): Expr = Expr.Tuple(t._1, t._2)

  implicit def tuple3(t: (Expr, Expr, Expr)): Expr = Expr.Tuple(t._1, t._2, t._3)

  implicit def tuple4(t: (Expr, Expr, Expr, Expr)): Expr = Expr.Tuple(t._1, t._2, t._3, t._4)

  implicit def tuple5(t: (Expr, Expr, Expr, Expr, Expr)): Expr = Expr.Tuple(t._1, t._2, t._3, t._4, t._5)

  implicit def tuple6(t: (Expr, Expr, Expr, Expr, Expr, Expr)): Expr = Expr.Tuple(t._1, t._2, t._3, t._4, t._5, t._6)

  // Lambda function.
  def lambda[T: ru.TypeTag, R: ru.TypeTag](f: T => R): Expr => Expr =
    (expr: Expr) => Expr.Lambda(expr, f)

  // Sequence of values.
  def seq(exprs: Expr*): Expr = Expr.List(exprs)

  // Aggregators.
  def sum(expr: Expr): Aggregator = Aggregator.Sum(expr)

  def count(expr: Expr): Aggregator = Aggregator.Count(expr)

  def collect(expr: Expr): Aggregator = Aggregator.Collect(expr)

  def one(expr: Expr): Aggregator = Aggregator.OneOf(expr)

  // Custom aggregator.
  def reducer[T: ru.TypeTag, U: ru.TypeTag](seed: U)(f: (T, U) => U): Expr => Aggregator =
    new Aggregator(_) {
      override protected def typeSpec(op: Specialized[_]): Reducer[_, _] = {
        val spec = op.typecheckAs[T]
        Reducer[T, U](spec, f, seed, Types.toInternalType(ru.typeOf[U]))
      }
    }

}
