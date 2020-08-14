package com.audienceproject.crossbow

import com.audienceproject.crossbow.expr.{Expr, ru}

import scala.language.implicitConversions

object Implicits {

  implicit class ColumnByName(val sc: StringContext) extends AnyVal {
    def $(args: Any*): Expr = Expr.Column(sc.s(args: _*))
  }

  implicit def lit2Expr[T: ru.TypeTag](value: T): Expr = Expr.Literal(value)

  implicit def tuple2Expr(t: (Expr, Expr)): Expr = Expr.Tuple(t._1, t._2)

  implicit def tuple3Expr(t: (Expr, Expr, Expr)): Expr = Expr.Tuple(t._1, t._2, t._3)

  implicit def tuple4Expr(t: (Expr, Expr, Expr, Expr)): Expr = Expr.Tuple(t._1, t._2, t._3, t._4)

  implicit def tuple5Expr(t: (Expr, Expr, Expr, Expr, Expr)): Expr = Expr.Tuple(t._1, t._2, t._3, t._4, t._5)

  implicit def tuple6Expr(t: (Expr, Expr, Expr, Expr, Expr, Expr)): Expr = Expr.Tuple(t._1, t._2, t._3, t._4, t._5, t._6)

  def lambda[T: ru.TypeTag, R: ru.TypeTag](f: T => R): Expr => Expr =
    (expr: Expr) => Expr.Lambda(expr, f)

  def seq(exprs: Expr*): Expr = Expr.List(exprs)

}
