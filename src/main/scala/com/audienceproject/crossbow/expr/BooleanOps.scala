package com.audienceproject.crossbow.expr

trait BooleanOps {

  self: Expr =>

  def not(): Expr = BooleanOps.Not(this)

  def &&(other: Expr): Expr = BooleanOps.And(this, other)

  def ||(other: Expr): Expr = BooleanOps.Or(this, other)

}

private object BooleanOps {

  case class Not(expr: Expr) extends UnaryExpr(expr) {
    override def typeSpec(operand: Specialized[_]): Specialized[Boolean] =
      if (operand.getType == "boolean") specialize[Boolean, Boolean](operand, !_)
      else throw new InvalidExpressionException("Not", operand.getType)
  }

  case class And(lhs: Expr, rhs: Expr) extends BinaryExpr(lhs, rhs) {
    override def typeSpec(lhsOperand: Specialized[_], rhsOperand: Specialized[_]): Specialized[Boolean] =
      (lhsOperand.getType, rhsOperand.getType) match {
        case ("boolean", "boolean") => specialize[Boolean, Boolean, Boolean](lhsOperand, rhsOperand, _ && _)
        case _ => throw new InvalidExpressionException("And", lhsOperand.getType, rhsOperand.getType)
      }
  }

  case class Or(lhs: Expr, rhs: Expr) extends BinaryExpr(lhs, rhs) {
    override def typeSpec(lhsOperand: Specialized[_], rhsOperand: Specialized[_]): Specialized[Boolean] =
      (lhsOperand.getType, rhsOperand.getType) match {
        case ("boolean", "boolean") => specialize[Boolean, Boolean, Boolean](lhsOperand, rhsOperand, _ || _)
        case _ => throw new InvalidExpressionException("Or", lhsOperand.getType, rhsOperand.getType)
      }
  }

}
