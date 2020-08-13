package com.audienceproject.crossbow.exceptions

import com.audienceproject.crossbow.expr.ru

class InvalidExpressionException(op: String, args: ru.Type*)
  extends RuntimeException(s"Invalid expression: $op(${args.mkString(",")}) - please check column types.")
