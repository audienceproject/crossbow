package com.audienceproject.crossbow

package object expr {

  private[crossbow] val ru = scala.reflect.runtime.universe

  sealed trait Type

  case object IntType extends Type {
    override def toString: String = "int"
  }

  case object LongType extends Type {
    override def toString: String = "long"
  }

  case object DoubleType extends Type {
    override def toString: String = "double"
  }

  case object BooleanType extends Type {
    override def toString: String = "boolean"
  }

  case class AnyType(runtimeType: ru.Type) extends Type {
    override def toString: String = runtimeType.toString
  }

  case class ProductType(elementTypes: Type*) extends Type {
    override def toString: String = s"(${elementTypes.mkString(",")})"
  }

  case class ListType(elementType: Type) extends Type {
    override def toString: String = s"List($elementType)"
  }

}
