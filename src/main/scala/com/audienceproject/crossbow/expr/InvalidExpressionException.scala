package com.audienceproject.crossbow.expr

class InvalidExpressionException(op: String, args: ru.Type*)
  extends RuntimeException(s"Invalid expression: $op(${args.mkString(",")}) - please check column types.")
