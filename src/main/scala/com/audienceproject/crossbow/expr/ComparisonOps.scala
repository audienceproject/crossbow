package com.audienceproject.crossbow.expr

trait ComparisonOps {

  self: Expr =>

  def >(other: Expr): Expr = ComparisonOps.GreaterThan(this, other)

  def <(other: Expr): Expr = ComparisonOps.GreaterThan(other, this)

  def >=(other: Expr): Expr = (this < other).not()

  def <=(other: Expr): Expr = (this > other).not()

}

private object ComparisonOps {

  case class GreaterThan(lhs: Expr, rhs: Expr) extends BinaryExpr(lhs, rhs) {
    override def typeSpec(lhsOperand: Specialized[_], rhsOperand: Specialized[_]): Specialized[_] =
      (lhsOperand.getType, rhsOperand.getType) match {
        // Long
        case ("long", "long") => specialize[Long, Long, Boolean](lhsOperand, rhsOperand, _ > _)
        case ("long", "int") => specialize[Long, Int, Boolean](lhsOperand, rhsOperand, _ > _)
        case ("long", "double") => specialize[Long, Double, Boolean](lhsOperand, rhsOperand, _ > _)
        // Int
        case ("int", "long") => specialize[Int, Long, Boolean](lhsOperand, rhsOperand, _ > _)
        case ("int", "int") => specialize[Int, Int, Boolean](lhsOperand, rhsOperand, _ > _)
        case ("int", "double") => specialize[Int, Double, Boolean](lhsOperand, rhsOperand, _ > _)
        // Double
        case ("double", "long") => specialize[Double, Long, Boolean](lhsOperand, rhsOperand, _ > _)
        case ("double", "int") => specialize[Double, Int, Boolean](lhsOperand, rhsOperand, _ > _)
        case ("double", "double") => specialize[Double, Double, Boolean](lhsOperand, rhsOperand, _ > _)
        case _ => throw new InvalidExpressionException("GreaterThan", lhsOperand.getType, rhsOperand.getType)
      }
  }

}
