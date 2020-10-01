package com.audienceproject.crossbow.algorithms

import com.audienceproject.crossbow.expr._

import scala.annotation.tailrec

private[crossbow] object Traversal {

  /**
   * Collects the output of PartialFunction 'pf' for all nodes in the expression tree on which it is defined.
   * Traverses the expression tree depth-first and applies 'pf' recursively on all children where 'pf' is undefined
   * on the parent.
   *
   * @param expr root of the expression tree
   * @param pf   function defined on the subset of Expr types which should be collected in the output
   * @tparam T element type of output
   * @return list of output
   */
  def collect[T](expr: Expr, pf: PartialFunction[Expr, T]): Seq[T] = {
    @tailrec
    def dfs(stack: List[Expr], result: List[T]): List[T] = stack match {
      case head :: tail => pf.lift(head) match {
        case Some(t) => dfs(tail, t :: result)
        case None => head match {
          case BinaryExpr(lhs, rhs) => dfs(lhs :: rhs :: tail, result)
          case UnaryExpr(expr) => dfs(expr :: tail, result)
          case Aggregator(expr) => dfs(expr :: tail, result)
          case Expr.Named(_, expr) => dfs(expr :: tail, result)
          case Expr.Tuple(exprs@_*) => dfs(exprs.toList ++ tail, result)
          case Expr.List(exprs) => dfs(exprs.toList ++ tail, result)
          case lambda: Expr.Lambda[_, _] => dfs(lambda.expr :: tail, result)
          case _ => dfs(tail, result)
        }
      }
      case _ => result
    }

    dfs(List(expr), List.empty)
  }

  /**
   * Transforms the given expression tree, replacing all nodes on which PartialFunction 'pf' is defined with its output.
   *
   * @param expr root of expression tree
   * @param pf   function defined on the subset of Expr types which should be replaced
   * @return result of transformation
   */
  def transform(expr: Expr, pf: PartialFunction[Expr, Expr]): Expr = {
    def step(elem: Expr) = transform(elem, pf)

    pf.applyOrElse[Expr, Expr](expr, {
      case ArithmeticOps.Plus(lhs, rhs) => ArithmeticOps.Plus(step(lhs), step(rhs))
      case ArithmeticOps.Minus(lhs, rhs) => ArithmeticOps.Minus(step(lhs), step(rhs))
      case ArithmeticOps.Multiply(lhs, rhs) => ArithmeticOps.Multiply(step(lhs), step(rhs))
      case ArithmeticOps.Divide(lhs, rhs) => ArithmeticOps.Divide(step(lhs), step(rhs))
      case ArithmeticOps.Mod(lhs, rhs) => ArithmeticOps.Mod(step(lhs), step(rhs))
      case ArithmeticOps.Abs(expr) => ArithmeticOps.Abs(step(expr))
      case ArithmeticOps.Negate(expr) => ArithmeticOps.Negate(step(expr))
      case BaseOps.EqualTo(lhs, rhs) => BaseOps.EqualTo(step(lhs), step(rhs))
      case BooleanOps.Not(expr) => BooleanOps.Not(step(expr))
      case BooleanOps.And(lhs, rhs) => BooleanOps.And(step(lhs), step(rhs))
      case BooleanOps.Or(lhs, rhs) => BooleanOps.Or(step(lhs), step(rhs))
      case ComparisonOps.GreaterThan(lhs, rhs) => ComparisonOps.GreaterThan(step(lhs), step(rhs))
      case Expr.Named(name, expr) => Expr.Named(name, step(expr))
      case Expr.Tuple(exprs@_*) => Expr.Tuple(exprs.map(step): _*)
      case Expr.List(exprs) => Expr.List(exprs.map(step))
      case lambda: Expr.Lambda[_, _] => lambda.copy(step(lambda.expr))
      case other => other
    })
  }

}
