package com.audienceproject.crossbow.algorithms

import com.audienceproject.crossbow.expr.Expr

import scala.annotation.tailrec

private[crossbow] object Traversal:
  /**
   * Transforms the given expression tree, replacing all nodes on which PartialFunction 'pf' is defined with its output.
   *
   * @param expr root of expression tree
   * @param pf   function defined on the subset of Expr types which should be replaced
   * @return result of transformation
   */
  def transform(expr: Expr, pf: PartialFunction[Expr, Expr]): Expr =
    def step(elem: Expr) = transform(elem, pf)

    pf.applyOrElse(expr, {
      case Expr.Named(name, expr) => Expr.Named(name, step(expr))
      case Expr.List(exprs) => Expr.List(exprs.map(step))
      case unary: Expr.Unary[?, ?] => unary.copy(step(unary.expr))
      case binary: Expr.Binary[?, ?, ?] => binary.copy(step(binary.lhs), step(binary.rhs))
      case agg: Expr.Aggregate[?, ?] => agg.copy(step(agg.expr))
      case other => other
    })
