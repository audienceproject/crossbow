package com.audienceproject.crossbow.expr

import com.audienceproject.crossbow.exceptions.IncorrectTypeException

private[crossbow] abstract class Specialized[@specialized(Int, Long, Double, Boolean) +T](implicit t: ru.TypeTag[T]) {

  def apply(i: Int): T

  def as[A]: Specialized[A] = this.asInstanceOf[Specialized[A]]

  def typecheckAs[A](implicit t: ru.TypeTag[A]): Specialized[A] =
    if (typeOf <:< ru.typeOf[A]) as[A]
    else throw new IncorrectTypeException(ru.typeOf[A], typeOf)

  val typeOf: ru.Type = ru.typeOf[T]

}
