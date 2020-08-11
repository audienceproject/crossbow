package com.audienceproject.crossbow

import com.audienceproject.crossbow.expr.Expr

import scala.language.implicitConversions
import scala.reflect.runtime.{universe => ru}

object Implicits {

  implicit class ColumnByName(val sc: StringContext) extends AnyVal {
    def $(args: Any*): Expr = Expr.Column(sc.s(args))
  }

  implicit def lit2Expr[T](value: T)(implicit t: ru.TypeTag[T]): Expr = Expr.Literal(value)

  def lambda[T, R](f: T => R)(implicit t: ru.TypeTag[R]): Expr => Expr = (expr: Expr) => Expr.Lambda(expr, f)

}
