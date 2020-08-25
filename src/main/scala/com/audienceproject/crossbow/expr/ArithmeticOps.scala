package com.audienceproject.crossbow.expr

import com.audienceproject.crossbow.exceptions.InvalidExpressionException

trait ArithmeticOps {

  self: Expr =>

  def +(other: Expr): Expr = ArithmeticOps.Plus(this, other)

  def -(other: Expr): Expr = ArithmeticOps.Minus(this, other)

  def *(other: Expr): Expr = ArithmeticOps.Multiply(this, other)

  def /(other: Expr): Expr = ArithmeticOps.Divide(this, other)

  def %(other: Expr): Expr = ArithmeticOps.Mod(this, other)

  def abs(): Expr = ArithmeticOps.Abs(this)

  def negate(): Expr = ArithmeticOps.Negate(this)

}

private[crossbow] object ArithmeticOps {

  case class Plus(lhs: Expr, rhs: Expr) extends BinaryExpr(lhs, rhs) {
    override def typeSpec(lhsOperand: Specialized[_], rhsOperand: Specialized[_]): Specialized[_] =
      (lhsOperand.typeOf, rhsOperand.typeOf) match {
        // Long
        case (LongType, LongType) => specialize[Long, Long, Long](lhsOperand, rhsOperand, _ + _)
        case (LongType, IntType) => specialize[Long, Int, Long](lhsOperand, rhsOperand, _ + _)
        case (IntType, LongType) => specialize[Int, Long, Long](lhsOperand, rhsOperand, _ + _)
        // Int
        case (IntType, IntType) => specialize[Int, Int, Int](lhsOperand, rhsOperand, _ + _)
        // Double
        case (DoubleType, DoubleType) => specialize[Double, Double, Double](lhsOperand, rhsOperand, _ + _)
        case (DoubleType, LongType) => specialize[Double, Long, Double](lhsOperand, rhsOperand, _ + _)
        case (LongType, DoubleType) => specialize[Long, Double, Double](lhsOperand, rhsOperand, _ + _)
        case (DoubleType, IntType) => specialize[Double, Int, Double](lhsOperand, rhsOperand, _ + _)
        case (IntType, DoubleType) => specialize[Int, Double, Double](lhsOperand, rhsOperand, _ + _)
        case _ => throw new InvalidExpressionException("Plus", lhsOperand.typeOf, rhsOperand.typeOf)
      }
  }

  case class Minus(lhs: Expr, rhs: Expr) extends BinaryExpr(lhs, rhs) {
    override def typeSpec(lhsOperand: Specialized[_], rhsOperand: Specialized[_]): Specialized[_] =
      (lhsOperand.typeOf, rhsOperand.typeOf) match {
        // Long
        case (LongType, LongType) => specialize[Long, Long, Long](lhsOperand, rhsOperand, _ - _)
        case (LongType, IntType) => specialize[Long, Int, Long](lhsOperand, rhsOperand, _ - _)
        case (IntType, LongType) => specialize[Int, Long, Long](lhsOperand, rhsOperand, _ - _)
        // Int
        case (IntType, IntType) => specialize[Int, Int, Int](lhsOperand, rhsOperand, _ - _)
        // Double
        case (DoubleType, DoubleType) => specialize[Double, Double, Double](lhsOperand, rhsOperand, _ - _)
        case (DoubleType, LongType) => specialize[Double, Long, Double](lhsOperand, rhsOperand, _ - _)
        case (LongType, DoubleType) => specialize[Long, Double, Double](lhsOperand, rhsOperand, _ - _)
        case (DoubleType, IntType) => specialize[Double, Int, Double](lhsOperand, rhsOperand, _ - _)
        case (IntType, DoubleType) => specialize[Int, Double, Double](lhsOperand, rhsOperand, _ - _)
        case _ => throw new InvalidExpressionException("Minus", lhsOperand.typeOf, rhsOperand.typeOf)
      }
  }

  case class Multiply(lhs: Expr, rhs: Expr) extends BinaryExpr(lhs, rhs) {
    override def typeSpec(lhsOperand: Specialized[_], rhsOperand: Specialized[_]): Specialized[_] =
      (lhsOperand.typeOf, rhsOperand.typeOf) match {
        // Long
        case (LongType, LongType) => specialize[Long, Long, Long](lhsOperand, rhsOperand, _ * _)
        case (LongType, IntType) => specialize[Long, Int, Long](lhsOperand, rhsOperand, _ * _)
        case (IntType, LongType) => specialize[Int, Long, Long](lhsOperand, rhsOperand, _ * _)
        // Int
        case (IntType, IntType) => specialize[Int, Int, Int](lhsOperand, rhsOperand, _ * _)
        // Double
        case (DoubleType, DoubleType) => specialize[Double, Double, Double](lhsOperand, rhsOperand, _ * _)
        case (DoubleType, LongType) => specialize[Double, Long, Double](lhsOperand, rhsOperand, _ * _)
        case (LongType, DoubleType) => specialize[Long, Double, Double](lhsOperand, rhsOperand, _ * _)
        case (DoubleType, IntType) => specialize[Double, Int, Double](lhsOperand, rhsOperand, _ * _)
        case (IntType, DoubleType) => specialize[Int, Double, Double](lhsOperand, rhsOperand, _ * _)
        case _ => throw new InvalidExpressionException("Multiply", lhsOperand.typeOf, rhsOperand.typeOf)
      }
  }

  case class Divide(lhs: Expr, rhs: Expr) extends BinaryExpr(lhs, rhs) {
    override def typeSpec(lhsOperand: Specialized[_], rhsOperand: Specialized[_]): Specialized[_] =
      (lhsOperand.typeOf, rhsOperand.typeOf) match {
        // Long
        case (LongType, LongType) => specialize[Long, Long, Long](lhsOperand, rhsOperand, _ / _)
        case (LongType, IntType) => specialize[Long, Int, Long](lhsOperand, rhsOperand, _ / _)
        case (IntType, LongType) => specialize[Int, Long, Long](lhsOperand, rhsOperand, _ / _)
        // Int
        case (IntType, IntType) => specialize[Int, Int, Int](lhsOperand, rhsOperand, _ / _)
        // Double
        case (DoubleType, DoubleType) => specialize[Double, Double, Double](lhsOperand, rhsOperand, _ / _)
        case (DoubleType, LongType) => specialize[Double, Long, Double](lhsOperand, rhsOperand, _ / _)
        case (LongType, DoubleType) => specialize[Long, Double, Double](lhsOperand, rhsOperand, _ / _)
        case (DoubleType, IntType) => specialize[Double, Int, Double](lhsOperand, rhsOperand, _ / _)
        case (IntType, DoubleType) => specialize[Int, Double, Double](lhsOperand, rhsOperand, _ / _)
        case _ => throw new InvalidExpressionException("Divide", lhsOperand.typeOf, rhsOperand.typeOf)
      }
  }

  case class Mod(lhs: Expr, rhs: Expr) extends BinaryExpr(lhs, rhs) {
    override def typeSpec(lhsOperand: Specialized[_], rhsOperand: Specialized[_]): Specialized[_] =
      (lhsOperand.typeOf, rhsOperand.typeOf) match {
        // Long
        case (LongType, LongType) => specialize[Long, Long, Long](lhsOperand, rhsOperand, _ % _)
        case (LongType, IntType) => specialize[Long, Int, Long](lhsOperand, rhsOperand, _ % _)
        case (IntType, LongType) => specialize[Int, Long, Long](lhsOperand, rhsOperand, _ % _)
        // Int
        case (IntType, IntType) => specialize[Int, Int, Int](lhsOperand, rhsOperand, _ % _)
        // Double
        case (DoubleType, DoubleType) => specialize[Double, Double, Double](lhsOperand, rhsOperand, _ % _)
        case (DoubleType, LongType) => specialize[Double, Long, Double](lhsOperand, rhsOperand, _ % _)
        case (LongType, DoubleType) => specialize[Long, Double, Double](lhsOperand, rhsOperand, _ % _)
        case (DoubleType, IntType) => specialize[Double, Int, Double](lhsOperand, rhsOperand, _ % _)
        case (IntType, DoubleType) => specialize[Int, Double, Double](lhsOperand, rhsOperand, _ % _)
        case _ => throw new InvalidExpressionException("Mod", lhsOperand.typeOf, rhsOperand.typeOf)
      }
  }

  case class Abs(expr: Expr) extends UnaryExpr(expr) {
    override def typeSpec(operand: Specialized[_]): Specialized[_] = operand.typeOf match {
      case LongType => specialize[Long, Long](operand, math.abs)
      case IntType => specialize[Int, Int](operand, math.abs)
      case DoubleType => specialize[Double, Double](operand, math.abs)
      case _ => throw new InvalidExpressionException("Abs", operand.typeOf)
    }
  }

  case class Negate(expr: Expr) extends UnaryExpr(expr) {
    override def typeSpec(operand: Specialized[_]): Specialized[_] = operand.typeOf match {
      case LongType => specialize[Long, Long](operand, -_)
      case IntType => specialize[Int, Int](operand, -_)
      case DoubleType => specialize[Double, Double](operand, -_)
      case _ => throw new InvalidExpressionException("Negate", operand.typeOf)
    }
  }

}
