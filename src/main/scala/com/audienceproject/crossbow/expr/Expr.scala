package com.audienceproject.crossbow.expr

import com.audienceproject.crossbow.DataFrame
import com.audienceproject.crossbow.exceptions.{IncorrectTypeException, InvalidExpressionException}

sealed trait Expr:
  private[crossbow] val typeOf: RuntimeType

  private[crossbow] def eval(i: Int): Any

  private[crossbow] def as[T]: Int => T = eval.asInstanceOf[Int => T]

  private[crossbow] def typecheckAs[T: TypeTag]: Int => T =
    val expectedType = summon[TypeTag[T]].runtimeType
    if (expectedType == typeOf) as[T]
    else throw new IncorrectTypeException(expectedType, typeOf)

private[crossbow] object Expr:

  case class Named(name: String, expr: Expr) extends Expr:
    override private[crossbow] val typeOf = expr.typeOf
    override private[crossbow] def eval(i: Int) = expr.eval(i)

  case class Cell(columnName: String)(using df: DataFrame) extends Expr:
    private val columnData = summon[DataFrame].getColumnData(columnName)
    override private[crossbow] val typeOf = summon[DataFrame].schema.get(columnName).columnType
    override private[crossbow] def eval(i: Int) = columnData.apply(i)
    override def toString: String = columnName

  case class Index()(using df: DataFrame) extends Expr:
    override private[crossbow] val typeOf = RuntimeType.Int
    override private[crossbow] def eval(i: Int): Int = i

  case class Literal[T: TypeTag](value: T) extends Expr:
    override private[crossbow] val typeOf = summon[TypeTag[T]].runtimeType
    override private[crossbow] def eval(i: Int): T = value

  case class Unary[T: TypeTag, R: TypeTag](expr: Expr, f: T => R) extends Expr:
    private val exprEval = expr.typecheckAs[T]
    override private[crossbow] val typeOf = summon[TypeTag[R]].runtimeType
    override private[crossbow] def eval(i: Int) = f(exprEval(i))
    private[crossbow] def copy(newExpr: Expr) = Unary(newExpr, f)

  case class Binary[T: TypeTag, U: TypeTag, R: TypeTag](lhs: Expr, rhs: Expr, f: (T, U) => R) extends Expr:
    private val lhsEval = lhs.typecheckAs[T]
    private val rhsEval = rhs.typecheckAs[U]
    override private[crossbow] val typeOf = summon[TypeTag[R]].runtimeType
    override private[crossbow] def eval(i: Int) = f(lhsEval(i), rhsEval(i))
    private[crossbow] def copy(newLhs: Expr, newRhs: Expr) = Binary(newLhs, newRhs, f)

  case class Aggregate[T: TypeTag, U: TypeTag](expr: Expr, f: (T, U) => U, seed: U) extends Expr:
    private val exprEval = expr.typecheckAs[T]
    override private[crossbow] val typeOf = summon[TypeTag[U]].runtimeType
    override private[crossbow] def eval(i: Int) = exprEval(i)
    private[crossbow] def reduce(i: Int, agg: U): U = f(exprEval(i), agg)
    private[crossbow] def copy(newExpr: Expr) = Aggregate(newExpr, f, seed)

  case class List(exprs: Seq[Expr]) extends Expr:
    override private[crossbow] val typeOf =
      val types = exprs.map(_.typeOf)
      if (types.distinct.size > 1) throw new InvalidExpressionException("List", exprs: _*)
      else RuntimeType.List(types.head)
    override private[crossbow] def eval(i: Int): Seq[Any] = exprs.map(_.eval(i))
