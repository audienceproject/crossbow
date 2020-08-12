package com.audienceproject.crossbow

import com.audienceproject.crossbow.expr.Expr
import com.audienceproject.crossbow.schema.{Column, Schema}

import scala.reflect.runtime.{universe => ru}

class DataFrame private(private val columnData: List[Array[_]],
                        val schema: Schema) extends Iterable[Seq[Any]] {

  val rowCount: Int = columnData.head.length
  val numColumns: Int = columnData.size

  def apply(index: Int): Seq[Any] = columnData.map(_ (index))

  def apply(range: Range): DataFrame = slice(range)

  def apply(columnNames: String*): DataFrame = {
    val colExprs = columnNames.map(Expr.Column)
    select(colExprs: _*)
  }

  def as[T]: TypedView[T] = {
    // TODO: Typecheck.
    new TypedView[T]
  }

  def addColumn(expr: Expr): DataFrame = {
    val op = expr.compile(this)
    val newCol = Array.from((0 until rowCount) map op.apply)
    val newColSchema = expr match {
      case Expr.Named(columnName, _) => new Column(columnName, op.typeOf)
      case _ => new Column(s"_$numColumns", op.typeOf)
    }
    new DataFrame(newCol :: columnData, schema.add(newColSchema))
  }

  def select(exprs: Expr*): DataFrame = {
    val (colData, colSchemas) = exprs.zipWithIndex.map({
      case (Expr.Named(newName, Expr.Column(colName)), _) => (getColumnData(colName), schema.get(colName).renamed(newName))
      case (Expr.Column(colName), _) => (getColumnData(colName), schema.get(colName))
      case (expr, i) =>
        val op = expr.compile(this)
        val newColSchema = expr match {
          case Expr.Named(columnName, _) => new Column(columnName, op.typeOf)
          case _ => new Column(s"_$i", op.typeOf)
        }
        (Array.from((0 until rowCount) map op.apply), newColSchema)
    }).unzip
    new DataFrame(colData.toList, Schema(colSchemas.toList))
  }

  def filter(expr: Expr): DataFrame = {
    // TODO: Typecheck.
    val op = expr.compile(this).as[Boolean]
    val indices = for (i <- 0 until rowCount if op(i)) yield i
    slice(indices)
  }

  def renameColumns(newNames: String*): DataFrame = {
    if (newNames.size != numColumns) throw new IllegalArgumentException("Wrong number of column names given.")
    val columnSchemas = schema.columns.zip(newNames).map({ case (col, name) => col.renamed(name) })
    new DataFrame(columnData, Schema(columnSchemas))
  }

  def renameColumns(toNewName: String => String): DataFrame = {
    val columnSchemas = schema.columns.map(col => col.renamed(toNewName(col.name)))
    new DataFrame(columnData, Schema(columnSchemas))
  }

  def printSchema(): Unit = println(schema)

  private def slice(indices: IndexedSeq[Int]): DataFrame = {
    val newData = List.fill(numColumns)(new Array[Any](indices.size))
    for ((oldIndex, newIndex) <- indices.zipWithIndex)
      for (c <- columnData.indices)
        newData(c)(newIndex) = columnData(c)(oldIndex)
    new DataFrame(newData, schema)
  }

  private[crossbow] def getColumnData(columnName: String): Array[_] = {
    val columnIndex = schema.indexOf(columnName)
    columnData(columnIndex)
  }

  class TypedView[T] extends Iterable[T] {
    private implicit val t2Tuple: Seq[Any] => T = toTuple[T](numColumns)

    def apply(index: Int): T = DataFrame.this (index)

    def apply(range: Range): Seq[T] = for (i <- range) yield this (i)

    override def iterator: Iterator[T] = this (0 until rowCount).iterator
  }

  override def iterator: Iterator[Seq[Any]] = (for (i <- 0 until rowCount) yield this (i)).iterator

}

object DataFrame {

  def fromSeq[T](data: Seq[T])(implicit t: ru.TypeTag[T]): DataFrame = {
    if (data.isEmpty) new DataFrame(List.empty, Schema())
    else {
      val dataType = ru.typeOf[T]
      if (dataType <:< ru.typeOf[Product]) {
        val tupleData = data.asInstanceOf[Seq[Product]]
        val tupleTypes = dataType.typeArgs
        val columnData = tupleTypes.zipWithIndex.map({
          case (t, i) if t =:= ru.typeOf[Int] => tupleData.map(_.productElement(i).asInstanceOf[Int]).toArray
          case (t, i) if t =:= ru.typeOf[Long] => tupleData.map(_.productElement(i).asInstanceOf[Long]).toArray
          case (t, i) if t =:= ru.typeOf[Double] => tupleData.map(_.productElement(i).asInstanceOf[Double]).toArray
          case (t, i) if t =:= ru.typeOf[Boolean] => tupleData.map(_.productElement(i).asInstanceOf[Boolean]).toArray
          case (_, i) => tupleData.map(_.productElement(i)).toArray
        })
        val columnSchemas = tupleTypes.zipWithIndex.map({ case (elementType, i) => new Column(s"_$i", elementType) })
        new DataFrame(columnData, Schema(columnSchemas))
      } else {
        // TODO: Specialized types.
        val col = Array.newBuilder[Any]
        for (d <- data) col += d
        new DataFrame(List(col.result()), Schema(List(new Column("_0", dataType))))
      }
    }
  }

}
