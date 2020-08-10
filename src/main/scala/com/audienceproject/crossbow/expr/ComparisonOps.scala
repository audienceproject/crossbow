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
    override def compile(): Specialized[_] = (lhsOperand.getType, rhsOperand.getType) match {
      // Long
      case ("long", "long") => specialize[Long, Long, Boolean](_ > _)
      case ("long", "int") => specialize[Long, Int, Boolean](_ > _)
      case ("long", "double") => specialize[Long, Double, Boolean](_ > _)
      // Int
      case ("int", "long") => specialize[Int, Long, Boolean](_ > _)
      case ("int", "int") => specialize[Int, Int, Boolean](_ > _)
      case ("int", "double") => specialize[Int, Double, Boolean](_ > _)
      // Double
      case ("double", "long") => specialize[Double, Long, Boolean](_ > _)
      case ("double", "int") => specialize[Double, Int, Boolean](_ > _)
      case ("double", "double") => specialize[Double, Double, Boolean](_ > _)
      case _ => throw new InvalidExpressionException("GreaterThan", lhsOperand.getType, rhsOperand.getType)
    }
  }

}
