package com.audienceproject.crossbow.expr

import scala.annotation.targetName

trait BaseOps:

  x: Expr =>

  @targetName("equals")
  def ===(y: Expr): Expr = Expr.Binary[Any, Any, Boolean](x, y, _ == _)

  @targetName("notEquals")
  def =!=(y: Expr): Expr = (x === y).not

  def as(name: String): Expr = Expr.Named(name, x)
