package com.audienceproject.crossbow.expr

import com.audienceproject.crossbow.exceptions.InvalidExpressionException

import scala.annotation.targetName

extension (x: Expr)

  @targetName("gt")
  def >(y: Expr): Expr = (x.typeOf, y.typeOf) match
    // Long
    case (RuntimeType.Long, RuntimeType.Long) => Expr.Binary[Long, Long, Boolean](x, y, _ > _)
    case (RuntimeType.Long, RuntimeType.Int) => Expr.Binary[Long, Int, Boolean](x, y, _ > _)
    case (RuntimeType.Long, RuntimeType.Double) => Expr.Binary[Long, Double, Boolean](x, y, _ > _)
    // Int
    case (RuntimeType.Int, RuntimeType.Long) => Expr.Binary[Int, Long, Boolean](x, y, _ > _)
    case (RuntimeType.Int, RuntimeType.Int) => Expr.Binary[Int, Int, Boolean](x, y, _ > _)
    case (RuntimeType.Int, RuntimeType.Double) => Expr.Binary[Int, Double, Boolean](x, y, _ > _)
    // Double
    case (RuntimeType.Double, RuntimeType.Long) => Expr.Binary[Double, Long, Boolean](x, y, _ > _)
    case (RuntimeType.Double, RuntimeType.Int) => Expr.Binary[Double, Int, Boolean](x, y, _ > _)
    case (RuntimeType.Double, RuntimeType.Double) => Expr.Binary[Double, Double, Boolean](x, y, _ > _)
    case _ => throw new InvalidExpressionException("gt", x, y)

  @targetName("lt")
  def <(y: Expr): Expr = y > x

  @targetName("gte")
  def >=(y: Expr): Expr = (x < y).not

  @targetName("lte")
  def <=(y: Expr): Expr = (x > y).not
