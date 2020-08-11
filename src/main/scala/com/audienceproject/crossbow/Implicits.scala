package com.audienceproject.crossbow

import com.audienceproject.crossbow.expr.Expr

import scala.language.implicitConversions
import scala.reflect.ClassTag

object Implicits {

  implicit class ColumnByName(val sc: StringContext) extends AnyVal {
    def $(args: Any*): Expr = Expr.Column(sc.s(args))
  }

  implicit def lit2Expr[T: ClassTag](value: T): Expr = Expr.Literal(value)

}
