package com.audienceproject.crossbow.exceptions

import com.audienceproject.crossbow.expr.{Type, ru}

class IncorrectTypeException(expected: ru.Type, actual: Type)
  extends RuntimeException(s"Expected $expected, but Expr type was $actual")
