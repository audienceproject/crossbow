package com.audienceproject.crossbow.expr

trait BaseOps {

  self: Expr =>

  def ===(other: Expr): Expr = BaseOps.EqualTo(this, other)

  def =!=(other: Expr): Expr = BaseOps.EqualTo(this, other).not()

  def as(name: String): Expr = Expr.Named(name, this)

}

private[crossbow] object BaseOps {

  case class EqualTo(lhs: Expr, rhs: Expr) extends BinaryExpr(lhs, rhs) {
    override def typeSpec(lhsOperand: Specialized[_], rhsOperand: Specialized[_]): Specialized[_] =
      specialize[Any, Any, Boolean](lhsOperand, rhsOperand, _ == _)
  }

}
