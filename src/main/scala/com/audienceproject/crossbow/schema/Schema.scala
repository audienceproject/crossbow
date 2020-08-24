package com.audienceproject.crossbow.schema

case class Schema(columns: Seq[Column] = Seq.empty) {

  def add(column: Column): Schema = Schema(columns :+ column)

  def get(columnName: String): Column = columns.find(_.name == columnName).getOrElse(
    throw new NoSuchElementException(s"Schema does not contain a column with name '$columnName''")
  )

  def indexOf(columnName: String): Int = {
    val index = columns.indexWhere(_.name == columnName)
    if (index >= 0) index
    else throw new NoSuchElementException(s"Schema does not contain a column with name '$columnName''")
  }

  private[crossbow] def matchColumns(other: Schema): Seq[(Column, Either[(Int, Int), Either[Int, Int]])] = {
    val leftMatches = columns.zipWithIndex.map({
      case (thisCol, i) =>
        val j = other.columns.indexWhere(_.name == thisCol.name)
        if (j >= 0) {
          val otherCol = other.columns(j)
          if (thisCol.columnType != otherCol.columnType)
            throw new IllegalArgumentException(s"Columns $thisCol and $otherCol do not match. Please check schemas.")
          else (thisCol, Left(i, j))
        } else (thisCol, Right(Left(i)))
    })
    val rightMatches = other.columns.zipWithIndex.collect({
      case (otherCol, j) if !columns.exists(_.name == otherCol.name) => (otherCol, Right(Right(j)))
    })
    leftMatches ++ rightMatches
  }

  override def toString: String = columns.map(_.toString).mkString("\n")

}
