package com.audienceproject.crossbow

import com.audienceproject.crossbow.exceptions.IncorrectTypeException
import com.audienceproject.crossbow.expr._
import com.audienceproject.crossbow.schema.{Column, Schema}

import scala.collection.immutable.ArraySeq
import scala.reflect.ClassTag
import scala.util.Sorting

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

  def as[T: ru.TypeTag]: TypedView[T] = {
    val dataType = ru.typeOf[T]
    val schemaType =
      if (numColumns == 1) schema.columns.head.columnType
      else ProductType(schema.columns.map(_.columnType): _*)
    if (Types.typecheck(schemaType, dataType)) new TypedView[T]
    else throw new IncorrectTypeException(dataType, schemaType)
  }

  def addColumn(expr: Expr): DataFrame = {
    val op = expr.compile(this)
    val newCol = sliceColumn(op)
    val newColSchema = expr match {
      case Expr.Named(columnName, _) => Column(columnName, op.typeOf)
      case _ => Column(s"_$numColumns", op.typeOf)
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
        (sliceColumn(op), newColSchema)
    }).unzip
    new DataFrame(colData.toList, Schema(colSchemas.toList))
  }

  def filter(expr: Expr): DataFrame = {
    val op = expr.compile(this).typecheckAs[Boolean]
    val indices = for (i <- 0 until rowCount if op(i)) yield i
    slice(indices)
  }

  def sortBy(expr: Expr, givenOrderings: Order*): DataFrame = {
    val op = expr.compile(this)
    val ord = Order.getOrdering(op.typeOf, givenOrderings)
    val indices = Array.tabulate(rowCount)(identity)
    Sorting.quickSort[Int](indices)((x: Int, y: Int) => ord.compare(op(x), op(y)))
    slice(ArraySeq.unsafeWrapArray(indices))
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
    val newData = schema.columns.map(col => {
      val op = Expr.Column(col.name).compile(this)
      sliceColumn(op, indices)
    })
    new DataFrame(newData, schema)
  }

  private def sliceColumn(op: Specialized[_], indices: Seq[Int] = 0 until rowCount): Array[_] = {
    op.typeOf match {
      case IntType => fillArray[Int](indices, op.as[Int].apply)
      case LongType => fillArray[Long](indices, op.as[Long].apply)
      case DoubleType => fillArray[Double](indices, op.as[Double].apply)
      case BooleanType => fillArray[Boolean](indices, op.as[Boolean].apply)
      case _ => fillArray[Any](indices, op.apply)
    }
  }

  private def fillArray[T: ClassTag](indices: Seq[Int], getValue: Int => T): Array[T] = {
    val arr = new Array[T](indices.size)
    for (i <- arr.indices) arr(i) = getValue(indices(i))
    arr
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

  def fromSeq[T: ru.TypeTag](data: Seq[T]): DataFrame = {
    if (data.isEmpty) new DataFrame(List.empty, Schema())
    else {
      val dataType = Types.toInternalType(ru.typeOf[T])
      dataType match {
        case ProductType(elementTypes@_*) =>
          val tupleData = data.asInstanceOf[Seq[Product]]
          val columnData = elementTypes.zipWithIndex.map({ case (t, i) => convert(tupleData.map(_.productElement(i)), t) })
          val columnSchemas = elementTypes.zipWithIndex.map({ case (t, i) => Column(s"_$i", t) })
          new DataFrame(columnData.toList, Schema(columnSchemas.toList))
        case _ =>
          val col = convert(data, dataType)
          new DataFrame(List(col), Schema(List(new Column("_0", dataType))))
      }
    }
  }

  private def convert(data: Seq[Any], dataType: Type): Array[_] = {
    dataType match {
      case IntType => data.asInstanceOf[Seq[Int]].toArray
      case LongType => data.asInstanceOf[Seq[Long]].toArray
      case DoubleType => data.asInstanceOf[Seq[Double]].toArray
      case BooleanType => data.asInstanceOf[Seq[Boolean]].toArray
      case _ => data.toArray
    }
  }

}
