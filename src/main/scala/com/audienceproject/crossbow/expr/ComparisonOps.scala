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
      (lhsOperand.typeOf, rhsOperand.typeOf) match {
        // Long
        case (LongType, LongType) => specialize[Long, Long, Boolean](lhsOperand, rhsOperand, _ > _)
        case (LongType, IntType) => specialize[Long, Int, Boolean](lhsOperand, rhsOperand, _ > _)
        case (LongType, DoubleType) => specialize[Long, Double, Boolean](lhsOperand, rhsOperand, _ > _)
        // Int
        case (IntType, LongType) => specialize[Int, Long, Boolean](lhsOperand, rhsOperand, _ > _)
        case (IntType, IntType) => specialize[Int, Int, Boolean](lhsOperand, rhsOperand, _ > _)
        case (IntType, DoubleType) => specialize[Int, Double, Boolean](lhsOperand, rhsOperand, _ > _)
        // Double
        case (DoubleType, LongType) => specialize[Double, Long, Boolean](lhsOperand, rhsOperand, _ > _)
        case (DoubleType, IntType) => specialize[Double, Int, Boolean](lhsOperand, rhsOperand, _ > _)
        case (DoubleType, DoubleType) => specialize[Double, Double, Boolean](lhsOperand, rhsOperand, _ > _)
        case _ => throw new InvalidExpressionException("GreaterThan", lhsOperand.typeOf, rhsOperand.typeOf)
      }
  }

}
