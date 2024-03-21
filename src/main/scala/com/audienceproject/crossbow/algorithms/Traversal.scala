package com.audienceproject.crossbow.algorithms

import com.audienceproject.crossbow.expr.Expr

private[crossbow] object Traversal:

  /**
   * Extracts specific nodes from the expression tree, according to the PartialFunction 'pf'.  Nodes on which 'pf' is
   * defined will be extracted, and all others will be traversed recursively.
   *
   * @param expr root of the expression tree
   * @param pf   function defined on the subset of Expr types which should be collected
   * @tparam E common type of the Exprs in the resulting collection
   * @return List of Exprs
   */
  def collect[E <: Expr](expr: Expr)(pf: PartialFunction[Expr, E]): List[E] =
    pf.andThen(_ :: Nil).applyOrElse(expr, {
      case Expr.Named(name, expr) => collect(expr)(pf)
      case Expr.List(exprs) => exprs.toList.flatMap(collect(_)(pf))
      case unary: Expr.Unary[?, ?] => collect(unary)(pf)
      case binary: Expr.Binary[?, ?, ?] => collect(binary.lhs)(pf) ++ collect(binary.rhs)(pf)
      case agg: Expr.Aggregate[?, ?] => collect(agg)(pf)
      case other => Nil
    })

  /**
   * Transforms the given expression tree, replacing all nodes on which PartialFunction 'pf' is defined with its output.
   *
   * @param expr root of expression tree
   * @param pf   function defined on the subset of Expr types which should be replaced
   * @return result of transformation
   */
  def transform(expr: Expr)(pf: PartialFunction[Expr, Expr]): Expr =
    def step(elem: Expr) = transform(elem)(pf)

    pf.applyOrElse(expr, {
      case Expr.Named(name, expr) => Expr.Named(name, step(expr))
      case Expr.List(exprs) => Expr.List(exprs map step)
      case unary: Expr.Unary[?, ?] => unary.copy(step(unary.expr))
      case binary: Expr.Binary[?, ?, ?] => binary.copy(step(binary.lhs), step(binary.rhs))
      case agg: Expr.Aggregate[?, ?] => agg.copy(step(agg.expr))
      case other => other
    })
