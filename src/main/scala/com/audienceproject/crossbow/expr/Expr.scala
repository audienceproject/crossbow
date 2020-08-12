package com.audienceproject.crossbow.expr

import com.audienceproject.crossbow.DataFrame

abstract class Expr extends BaseOps with ArithmeticOps with BooleanOps with ComparisonOps {
  private[crossbow] def compile(context: DataFrame): Specialized[_]
}

private[crossbow] object Expr {

  case class Named(name: String, expr: Expr) extends Expr {
    override private[crossbow] def compile(context: DataFrame) = expr.compile(context)
  }

  case class Column(columnName: String) extends Expr {
    override private[crossbow] def compile(context: DataFrame) = {
      val columnType = context.schema.get(columnName).getTypeInternal
      columnType match {
        case IntType =>
          val columnData = context.getColumnData(columnName).asInstanceOf[Array[Int]]
          new Specialized[Int] {
            override def apply(i: Int): Int = columnData(i)
          }
        case LongType =>
          val columnData = context.getColumnData(columnName).asInstanceOf[Array[Long]]
          new Specialized[Long] {
            override def apply(i: Int): Long = columnData(i)
          }
        case DoubleType =>
          val columnData = context.getColumnData(columnName).asInstanceOf[Array[Double]]
          new Specialized[Double] {
            override def apply(i: Int): Double = columnData(i)
          }
        case BooleanType =>
          val columnData = context.getColumnData(columnName).asInstanceOf[Array[Boolean]]
          new Specialized[Boolean] {
            override def apply(i: Int): Boolean = columnData(i)
          }
        case _ =>
          val columnData = context.getColumnData(columnName)
          new Specialized[Any]() {
            override def apply(i: Int): Any = columnData(i)

            override val typeOf: ru.Type = columnType
          }
      }
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
