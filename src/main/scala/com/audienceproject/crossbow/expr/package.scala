package com.audienceproject.crossbow

import scala.reflect.api.JavaUniverse

package object expr {

  val ru: JavaUniverse = scala.reflect.runtime.universe

  val IntType: ru.Type = ru.typeOf[Int]
  val LongType: ru.Type = ru.typeOf[Long]
  val DoubleType: ru.Type = ru.typeOf[Double]
  val BooleanType: ru.Type = ru.typeOf[Boolean]

}
