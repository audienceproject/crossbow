package com.audienceproject.crossbow.schema

import scala.reflect.runtime.{universe => ru}

class Column(val name: String, private val columnType: ru.Type) {

  def isOf[T](implicit t: ru.TypeTag[T]): Boolean = columnType =:= ru.typeOf[T]

  override def equals(obj: Any): Boolean = obj match {
    case other: Column => this.name == other.name && this.columnType =:= other.columnType
    case _ => false
  }

  override def hashCode(): Int = name.hashCode + columnType.hashCode() * 13

}

object Column {

  def of[T](columnName: String)(implicit t: ru.TypeTag[T]): Column = new Column(columnName, ru.typeOf[T])

}
