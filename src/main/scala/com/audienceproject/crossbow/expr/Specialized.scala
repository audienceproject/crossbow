package com.audienceproject.crossbow.expr

private[crossbow] abstract class Specialized[@specialized(Int, Long, Double, Boolean) +T](implicit t: ru.TypeTag[T]) {

  def apply(i: Int): T

  def as[A]: Specialized[A] = this.asInstanceOf[Specialized[A]]

  val typeOf: ru.Type = ru.typeOf[T]

}
