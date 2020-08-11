package com.audienceproject.crossbow.expr

import com.audienceproject.crossbow.DataFrame

protected abstract class UnaryExpr(expr: Expr) extends Expr {

  override private[crossbow] def compile(context: DataFrame) = {
    val operand = expr.compile(context)
    typeSpec(operand)
  }

  def typeSpec(operand: Specialized[_]): Specialized[_]

  def specialize[T, U](operand: Specialized[_], op: T => U)(implicit t: ru.TypeTag[U]): Specialized[U] =
    UnaryExpr.UnaryOp[T, U](operand.as, op)

}

private object UnaryExpr {

  private[UnaryExpr] case class UnaryOp[T, U](operand: Specialized[T], op: T => U)
                                             (implicit t: ru.TypeTag[U]) extends Specialized[U] {
    override def apply(i: Int): U = op(operand(i))
  }

}
