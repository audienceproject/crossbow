package com.audienceproject.crossbow.expr

import com.audienceproject.crossbow.DataFrame
import com.audienceproject.crossbow.exceptions.InvalidExpressionException

abstract class Expr extends BaseOps with ArithmeticOps with BooleanOps with ComparisonOps {
  private[crossbow] def compile(context: DataFrame): Specialized[_]
}

private[crossbow] object Expr {

  case class Named(name: String, expr: Expr) extends Expr {
    override private[crossbow] def compile(context: DataFrame) = expr.compile(context)
  }

  case class Column(columnName: String) extends Expr {
    override private[crossbow] def compile(context: DataFrame) = {
      val columnType = context.schema.get(columnName).columnType
      columnType match {
        case IntType =>
          val columnData = context.getColumnData(columnName).asInstanceOf[Array[Int]]
          specialize[Int](columnData)
        case LongType =>
          val columnData = context.getColumnData(columnName).asInstanceOf[Array[Long]]
          specialize[Long](columnData)
        case DoubleType =>
          val columnData = context.getColumnData(columnName).asInstanceOf[Array[Double]]
          specialize[Double](columnData)
        case BooleanType =>
          val columnData = context.getColumnData(columnName).asInstanceOf[Array[Boolean]]
          specialize[Boolean](columnData)
        case _ =>
          val columnData = context.getColumnData(columnName)
          specializeWithType[Any](columnData, columnType)
      }
    }
  }

  case class Literal[T: ru.TypeTag](value: T) extends Expr {
    override private[crossbow] def compile(context: DataFrame) = specialize[T](_ => value)
  }

  case class Lambda[T: ru.TypeTag, R: ru.TypeTag](expr: Expr, f: T => R) extends Expr {
    override private[crossbow] def compile(context: DataFrame) = {
      val spec = expr.compile(context).typecheckAs[T]
      specialize[R](i => f(spec(i)))
    }
  }

  case class Tuple(exprs: Expr*) extends Expr {
    override private[crossbow] def compile(context: DataFrame) = {
      val elementSpecs = exprs.map(_.compile(context))
      val productType = ProductType(elementSpecs.map(_.typeOf): _*)
      elementSpecs match {
        case Seq(e1, e2) => specializeWithType[(_, _)](i => (e1(i), e2(i)), productType)
        case Seq(e1, e2, e3) => specializeWithType[(_, _, _)](i => (e1(i), e2(i), e3(i)), productType)
        case Seq(e1, e2, e3, e4) => specializeWithType[(_, _, _, _)](i => (e1(i), e2(i), e3(i), e4(i)), productType)
        case Seq(e1, e2, e3, e4, e5) => specializeWithType[(_, _, _, _, _)](i => (e1(i), e2(i), e3(i), e4(i), e5(i)), productType)
        case Seq(e1, e2, e3, e4, e5, e6) => specializeWithType[(_, _, _, _, _, _)](i => (e1(i), e2(i), e3(i), e4(i), e5(i), e6(i)), productType)
      }
    }
  }

  case class List(exprs: Seq[Expr]) extends Expr {
    override private[crossbow] def compile(context: DataFrame) = {
      if (exprs.isEmpty) specialize[Seq[Nothing]](_ => Seq.empty)
      else {
        val elementSpecs = exprs.map(_.compile(context))
        val elementTypes = elementSpecs.map(_.typeOf)
        if (elementTypes.distinct.size > 1) throw new InvalidExpressionException("List", elementTypes: _*)
        specializeWithType[Seq[_]](i => elementSpecs.map(_ (i)), ListType(elementTypes.head))
      }
    }
  }

  private def specialize[T: ru.TypeTag](op: Int => T) = new Specialized[T] {
    override def apply(i: Int): T = op(i)
  }

  private def specializeWithType[T: ru.TypeTag](op: Int => T, specializedType: Type) = new Specialized[T] {
    override def apply(i: Int): T = op(i)

    override val typeOf: Type = specializedType
  }

}
