package com.audienceproject.crossbow.expr

import scala.reflect.{ClassTag, classTag}

private[crossbow] abstract class Specialized[@specialized(Int, Long, Double) T: ClassTag] {

  def apply(i: Int): T

  def as[A]: Specialized[A] = this.asInstanceOf[Specialized[A]]

  def getType: String = classTag[T].getClass.getSimpleName

}
