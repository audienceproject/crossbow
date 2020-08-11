package com.audienceproject.crossbow.schema

case class Schema(columns: List[Column] = List.empty) {

  def indexOf(columnName: String): Int = columns.indexWhere(_.name == columnName)

}
