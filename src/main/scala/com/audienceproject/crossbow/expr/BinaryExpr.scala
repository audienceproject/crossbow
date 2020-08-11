package com.audienceproject.crossbow.expr

import com.audienceproject.crossbow.DataFrame

protected abstract class BinaryExpr(lhs: Expr, rhs: Expr) extends Expr {

  override private[crossbow] def compile(context: DataFrame) = {
    val lhsOperand = lhs.compile(context)
    val rhsOperand = rhs.compile(context)
    typeSpec(lhsOperand, rhsOperand)
  }

  def typeSpec(lhsOperand: Specialized[_], rhsOperand: Specialized[_]): Specialized[_]

  def specialize[T, U, V](lhsOperand: Specialized[_], rhsOperand: Specialized[_], op: (T, U) => V)
                         (implicit t: ru.TypeTag[V]): Specialized[V] =
    BinaryExpr.BinaryOp[T, U, V](lhsOperand.as, rhsOperand.as, op)

}

private object BinaryExpr {

  private[BinaryExpr] case class BinaryOp[T, U, V](lhs: Specialized[T], rhs: Specialized[U], op: (T, U) => V)
                                                  (implicit t: ru.TypeTag[V]) extends Specialized[V] {
    override def apply(i: Int): V = op(lhs(i), rhs(i))
  }

}
