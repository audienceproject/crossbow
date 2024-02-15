package com.audienceproject.crossbow.schema

import com.audienceproject.crossbow.expr.{RuntimeType, TypeTag}

case class Column private[crossbow](name: String, columnType: RuntimeType):

  def renamed(newName: String): Column = Column(newName, columnType)

  override def toString: String = s"$name: $columnType"

object Column:

  def apply[T: TypeTag](columnName: String): Column =
    Column(columnName, summon[TypeTag[T]].runtimeType)
