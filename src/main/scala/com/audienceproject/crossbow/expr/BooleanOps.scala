package com.audienceproject.crossbow.expr

import com.audienceproject.crossbow.exceptions.InvalidExpressionException

import scala.annotation.targetName

trait BooleanOps:

  x: Expr =>

  def not: Expr = x.typeOf match
    case RuntimeType.Boolean => Expr.Unary[Boolean, Boolean](x, !_)
    case _ => throw new InvalidExpressionException("not", x)

  @targetName("and")
  def &&(y: Expr): Expr = (x.typeOf, y.typeOf) match
    case (RuntimeType.Boolean, RuntimeType.Boolean) => Expr.Binary[Boolean, Boolean, Boolean](x, y, _ && _)
    case _ => throw new InvalidExpressionException("and", x, y)

  @targetName("or")
  def ||(y: Expr): Expr = (x.typeOf, y.typeOf) match
    case (RuntimeType.Boolean, RuntimeType.Boolean) => Expr.Binary[Boolean, Boolean, Boolean](x, y, _ || _)
    case _ => throw new InvalidExpressionException("or", x, y)
