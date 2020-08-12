package com.audienceproject.crossbow.schema

import scala.reflect.runtime.{universe => ru}

class Column private[crossbow](val name: String, private val columnType: ru.Type) {

  def isOf[T](implicit t: ru.TypeTag[T]): Boolean = columnType =:= ru.typeOf[T]

  def conformsTo[T](implicit t: ru.TypeTag[T]): Boolean = columnType <:< ru.typeOf[T]

  def renamed(newName: String): Column = new Column(newName, columnType)

  private[crossbow] def getTypeInternal: ru.Type = columnType

  override def equals(obj: Any): Boolean = obj match {
    case other: Column => this.name == other.name && this.columnType =:= other.columnType
    case _ => false
  }

  override def hashCode(): Int = name.hashCode + columnType.hashCode() * 13

  override def toString: String = s"$name: $columnType"

}

object Column {

  def of[T](columnName: String)(implicit t: ru.TypeTag[T]): Column = new Column(columnName, ru.typeOf[T])

}
