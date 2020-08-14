package com.audienceproject.crossbow.expr

import com.audienceproject.crossbow.exceptions.IncorrectTypeException

private[crossbow] abstract class Specialized[@specialized(Int, Long, Double, Boolean) +T: ru.TypeTag] {

  def apply(i: Int): T

  def as[A]: Specialized[A] = this.asInstanceOf[Specialized[A]]

  def typecheckAs[A: ru.TypeTag]: Specialized[A] = {
    val asType = ru.typeOf[A]
    if (Types.typecheck(typeOf, asType)) as[A]
    else throw new IncorrectTypeException(asType, typeOf)
  }

  val typeOf: Type = Types.toInternalType(ru.typeOf[T])

}
