package com.audienceproject.crossbow.expr

import com.audienceproject.crossbow.exceptions.InvalidExpressionException

trait BooleanOps {

  self: Expr =>

  def not(): Expr = BooleanOps.Not(this)

  def &&(other: Expr): Expr = BooleanOps.And(this, other)

  def ||(other: Expr): Expr = BooleanOps.Or(this, other)

}

private object BooleanOps {

  case class Not(expr: Expr) extends UnaryExpr(expr) {
    override def typeSpec(operand: Specialized[_]): Specialized[Boolean] =
      if (operand.typeOf =:= BooleanType) specialize[Boolean, Boolean](operand, !_)
      else throw new InvalidExpressionException("Not", operand.typeOf)
  }

  case class And(lhs: Expr, rhs: Expr) extends BinaryExpr(lhs, rhs) {
    override def typeSpec(lhsOperand: Specialized[_], rhsOperand: Specialized[_]): Specialized[Boolean] =
      (lhsOperand.typeOf, rhsOperand.typeOf) match {
        case (BooleanType, BooleanType) => specialize[Boolean, Boolean, Boolean](lhsOperand, rhsOperand, _ && _)
        case _ => throw new InvalidExpressionException("And", lhsOperand.typeOf, rhsOperand.typeOf)
      }
  }

  case class Or(lhs: Expr, rhs: Expr) extends BinaryExpr(lhs, rhs) {
    override def typeSpec(lhsOperand: Specialized[_], rhsOperand: Specialized[_]): Specialized[Boolean] =
      (lhsOperand.typeOf, rhsOperand.typeOf) match {
        case (BooleanType, BooleanType) => specialize[Boolean, Boolean, Boolean](lhsOperand, rhsOperand, _ || _)
        case _ => throw new InvalidExpressionException("Or", lhsOperand.typeOf, rhsOperand.typeOf)
      }
  }

}
