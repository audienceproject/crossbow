package com.audienceproject.crossbow.schema

case class Schema(columns: List[Column] = List.empty) {

  def add(column: Column): Schema = Schema(column :: columns)

  def get(columnName: String): Column = columns.find(_.name == columnName).getOrElse(
    throw new NoSuchElementException(s"Schema does not contain a column with name '$columnName''")
  )

  def indexOf(columnName: String): Int = columns.indexWhere(_.name == columnName)

  override def toString: String = columns.map(_.toString).mkString("\n")

}
