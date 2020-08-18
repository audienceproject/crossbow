package com.audienceproject.crossbow

import com.audienceproject.crossbow.expr.{Expr, ru}

import scala.language.implicitConversions

object Implicits {

  implicit class ColumnByName(val sc: StringContext) extends AnyVal {
    def $(args: Any*): Expr = Expr.Column(sc.s(args: _*))
  }

  implicit def lit[T: ru.TypeTag](value: T): Expr = Expr.Literal(value)

  implicit def tuple2(t: (Expr, Expr)): Expr = Expr.Tuple(t._1, t._2)

  implicit def tuple3(t: (Expr, Expr, Expr)): Expr = Expr.Tuple(t._1, t._2, t._3)

  implicit def tuple4(t: (Expr, Expr, Expr, Expr)): Expr = Expr.Tuple(t._1, t._2, t._3, t._4)

  implicit def tuple5(t: (Expr, Expr, Expr, Expr, Expr)): Expr = Expr.Tuple(t._1, t._2, t._3, t._4, t._5)

  implicit def tuple6(t: (Expr, Expr, Expr, Expr, Expr, Expr)): Expr = Expr.Tuple(t._1, t._2, t._3, t._4, t._5, t._6)

  def lambda[T: ru.TypeTag, R: ru.TypeTag](f: T => R): Expr => Expr =
    (expr: Expr) => Expr.Lambda(expr, f)

  def seq(exprs: Expr*): Expr = Expr.List(exprs)

}
