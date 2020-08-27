package com.audienceproject.crossbow.expr

import com.audienceproject.crossbow.DataFrame

protected abstract class UnaryExpr(private val expr: Expr) extends Expr {

  override private[crossbow] def compile(context: DataFrame) = {
    val operand = expr.compile(context)
    typeSpec(operand)
  }

  def typeSpec(operand: Specialized[_]): Specialized[_]

  def specialize[T, U: ru.TypeTag](operand: Specialized[_], op: T => U): Specialized[U] =
    UnaryExpr.UnaryOp[T, U](operand.as, op)

}

private[crossbow] object UnaryExpr {

  private[UnaryExpr] case class UnaryOp[T, U: ru.TypeTag](operand: Specialized[T], op: T => U)
    extends Specialized[U] {
    override def apply(i: Int): U = op(operand(i))
  }

  def unapply(arg: UnaryExpr): Option[Expr] = Some(arg.expr)

}
