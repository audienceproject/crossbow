package com.audienceproject.crossbow

import com.audienceproject.crossbow.expr.{Expr, ru}

import scala.language.implicitConversions

object Implicits {

  implicit class ColumnByName(val sc: StringContext) extends AnyVal {
    def $(args: Any*): Expr = Expr.Column(sc.s(args: _*))
  }

  implicit def lit2Expr[T](value: T)(implicit t: ru.TypeTag[T]): Expr = Expr.Literal(value)

  def lambda[T, R](f: T => R)(implicit t: ru.TypeTag[T], r: ru.TypeTag[R]): Expr => Expr =
    (expr: Expr) => Expr.Lambda(expr, f)

}
