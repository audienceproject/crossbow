package com.audienceproject.crossbow.exceptions

import com.audienceproject.crossbow.expr.Expr

class InvalidExpressionException(op: String, args: Expr*)
  extends RuntimeException(s"Invalid expression: $op(${args.map(_.typeOf).mkString(",")}) - please check column types.")
