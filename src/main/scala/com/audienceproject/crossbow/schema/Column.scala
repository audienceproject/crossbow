package com.audienceproject.crossbow.schema

import com.audienceproject.crossbow.expr.{Type, Types, ru}

case class Column private[crossbow](name: String, columnType: Type) {

  def renamed(newName: String): Column = Column(newName, columnType)

  override def toString: String = s"$name: $columnType"

}

object Column {

  def apply[T: ru.TypeTag](columnName: String): Column =
    Column(columnName, Types.toInternalType(ru.typeOf[T]))

}
