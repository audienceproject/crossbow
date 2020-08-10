package com.audienceproject.crossbow.expr

import scala.reflect.ClassTag

protected abstract class UnaryExpr(expr: Expr) extends Expr {

  protected val operand: Specialized[_] = expr.compile()

  protected def specialize[T, U: ClassTag](op: T => U): Specialized[U] = UnaryExpr.UnaryOp[T, U](operand.as, op)

}

private object UnaryExpr {

  private[UnaryExpr] case class UnaryOp[T, U: ClassTag](operand: Specialized[T], op: T => U)
    extends Specialized[U] {
    override def apply(i: Int): U = op(operand(i))
  }

}
