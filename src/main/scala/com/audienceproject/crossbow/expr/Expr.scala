package com.audienceproject.crossbow.expr

import com.audienceproject.crossbow.DataFrame

import scala.reflect.ClassTag

abstract class Expr extends BaseOps with ArithmeticOps with BooleanOps with ComparisonOps {
  private[crossbow] def compile(context: DataFrame): Specialized[_]
}

private[crossbow] object Expr {

  case class Column(columnName: String) extends Expr {
    // TODO: Check schema first for type specialization.
    override private[crossbow] def compile(context: DataFrame) = new Specialized[Any] {
      private val column = context.getColumn(columnName)

      override def apply(i: Int): Any = column(i)

      override def getType: String = "" // TODO: Check schema for type.
    }
  }

  case class Literal[T: ClassTag](value: T) extends Expr {
    override private[crossbow] def compile(context: DataFrame) = new Specialized[T] {
      override def apply(i: Int): T = value
    }
  }

}
