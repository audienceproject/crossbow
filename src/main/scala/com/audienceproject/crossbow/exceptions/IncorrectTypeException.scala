package com.audienceproject.crossbow.exceptions

import com.audienceproject.crossbow.expr.RuntimeType

class IncorrectTypeException(expected: RuntimeType, actual: RuntimeType)
  extends RuntimeException(s"Expected $expected, but runtime type was $actual")
