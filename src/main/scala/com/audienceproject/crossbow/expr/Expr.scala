package com.audienceproject.crossbow.expr

import com.audienceproject.crossbow.DataFrame

abstract class Expr extends BaseOps with ArithmeticOps with BooleanOps with ComparisonOps {
  private[crossbow] def compile(context: DataFrame): Specialized[_]
}

private[crossbow] object Expr {

  case class Column(columnName: String) extends Expr {
    // TODO: Check schema first for type specialization.
    override private[crossbow] def compile(context: DataFrame) = new Specialized[Any] {
      private val column = context.getColumn(columnName)

      override val typeOf: ru.Type = ??? // TODO: Check schema for type.

      override def apply(i: Int): Any = column(i)
    }
  }

  case class Literal[T](value: T)(implicit t: ru.TypeTag[T]) extends Expr {
    override private[crossbow] def compile(context: DataFrame) = new Specialized[T] {
      override def apply(i: Int): T = value
    }
  }

  case class Lambda[T, R](expr: Expr, f: T => R)(implicit t: ru.TypeTag[R]) extends Expr {
    override private[crossbow] def compile(context: DataFrame) = {
      // TODO: Typecheck.
      val spec = expr.compile(context).as[T]
      new Specialized[R] {
        override def apply(i: Int): R = f(spec(i))
      }
    }
  }

}
