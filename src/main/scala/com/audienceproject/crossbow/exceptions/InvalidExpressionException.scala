package com.audienceproject.crossbow.exceptions

import com.audienceproject.crossbow.expr.Type

class InvalidExpressionException(op: String, args: Type*)
  extends RuntimeException(s"Invalid expression: $op(${args.mkString(",")}) - please check column types.")
