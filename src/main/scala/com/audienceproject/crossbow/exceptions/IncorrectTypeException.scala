package com.audienceproject.crossbow.exceptions

import com.audienceproject.crossbow.expr.RuntimeType

class IncorrectTypeException(expected: RuntimeType, actual: RuntimeType)
  extends RuntimeException(s"Expected $expected, but Expr type was $actual")
