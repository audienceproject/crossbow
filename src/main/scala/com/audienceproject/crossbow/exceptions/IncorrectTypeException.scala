package com.audienceproject.crossbow.exceptions

import com.audienceproject.crossbow.expr.ru

class IncorrectTypeException(message: String) extends RuntimeException(message) {

  def this(expected: ru.Type, actual: ru.Type) =
    this(s"Expected $expected, but Expr type was $actual")

}
