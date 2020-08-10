package com.audienceproject.crossbow.expr

import scala.reflect.ClassTag

protected abstract class BinaryExpr(lhs: Expr, rhs: Expr) extends Expr {

  protected val lhsOperand: Specialized[_] = lhs.compile()
  protected val rhsOperand: Specialized[_] = rhs.compile()

  protected def specialize[T, U, V: ClassTag](op: (T, U) => V): Specialized[V] =
    BinaryExpr.BinaryOp[T, U, V](lhsOperand.as, rhsOperand.as, op)

}

private object BinaryExpr {

  private[BinaryExpr] case class BinaryOp[T, U, V: ClassTag](lhs: Specialized[T], rhs: Specialized[U], op: (T, U) => V)
    extends Specialized[V] {
    override def apply(i: Int): V = op(lhs(i), rhs(i))
  }

}
