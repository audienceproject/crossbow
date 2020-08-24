package com.audienceproject.crossbow

import com.audienceproject.crossbow.algorithms.{GroupBy, SortMergeJoin}
import com.audienceproject.crossbow.exceptions.IncorrectTypeException
import com.audienceproject.crossbow.expr._
import com.audienceproject.crossbow.schema.{Column, Schema}

import scala.collection.immutable.ArraySeq
import scala.reflect.ClassTag
import scala.util.Sorting

class DataFrame private(private val columnData: Vector[Array[_]],
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
    new DataFrame(columnData :+ newCol, schema.add(newColSchema))
  }

  def removeColumns(columnNames: String*): DataFrame = {
    val remaining = for ((c, i) <- schema.columns.zipWithIndex if !columnNames.contains(c.name))
      yield (columnData(i), c)
    val (colData, colSchemas) = remaining.unzip
    new DataFrame(colData.toVector, Schema(colSchemas))
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
    new DataFrame(colData.toVector, Schema(colSchemas))
  }

  def filter(expr: Expr): DataFrame = {
    val op = expr.compile(this).typecheckAs[Boolean]
    val indices = for (i <- 0 until rowCount if op(i)) yield i
    slice(indices)
  }

  def groupBy(keyExprs: Expr*): GroupedView = new GroupedView(keyExprs)

  def sortBy(expr: Expr, givenOrderings: Order*): DataFrame = {
    val op = expr.compile(this)
    val ord = Order.getOrdering(op.typeOf, givenOrderings)
    val indices = Array.tabulate(rowCount)(identity)
    Sorting.quickSort[Int](indices)((x: Int, y: Int) => ord.compare(op(x), op(y)))
    slice(ArraySeq.unsafeWrapArray(indices))
  }

  def join(other: DataFrame, joinExpr: Expr, joinType: JoinType = JoinType.Inner): DataFrame =
    SortMergeJoin(this, other, joinExpr, joinType)

  def union(other: DataFrame): DataFrame = {
    if (schema == other.schema) {
      val colData = for (i <- 0 until numColumns) yield columnData(i) ++ other.columnData(i)
      new DataFrame(colData.toVector, schema)
    } else {
      val matchingColumns = schema.matchColumns(other.schema)
      val colData = matchingColumns.map({
        case (_, Left((i, j))) => columnData(i) ++ other.columnData(j)
        case (c, Right(Left(i))) => columnData(i) ++ fillNullArray(other.rowCount, c.columnType)
        case (c, Right(Right(j))) => fillNullArray(rowCount, c.columnType) ++ other.columnData(j)
      })
      new DataFrame(colData.toVector, Schema(matchingColumns.map(_._1)))
    }
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

  private[crossbow] def merge(other: DataFrame): DataFrame = {
    val otherColumns = other.schema.columns.map(c => c.renamed("#" + c.name))
    new DataFrame(columnData ++ other.columnData, Schema(schema.columns ++ otherColumns))
  }

  private[crossbow] def slice(indices: IndexedSeq[Int]): DataFrame = {
    val newData = schema.columns.map(col => {
      val op = Expr.Column(col.name).compile(this)
      sliceColumn(op, indices)
    })
    new DataFrame(newData.toVector, schema)
  }

  private def sliceColumn(op: Specialized[_], indices: Seq[Int] = 0 until rowCount): Array[_] = {
    op.typeOf match {
      case IntType => fillArray[Int](indices, op.as[Int].apply, 0)
      case LongType => fillArray[Long](indices, op.as[Long].apply, 0L)
      case DoubleType => fillArray[Double](indices, op.as[Double].apply, 0d)
      case BooleanType => fillArray[Boolean](indices, op.as[Boolean].apply, false)
      case _ => fillArray[Any](indices, op.apply, null)
    }
  }

  private def fillArray[T: ClassTag](indices: Seq[Int], getValue: Int => T, getNull: => T): Array[T] = {
    val arr = new Array[T](indices.size)
    for (i <- arr.indices)
      if (indices(i) >= 0) arr(i) = getValue(indices(i))
      else arr(i) = getNull
    arr
  }

  private def fillNullArray(size: Int, ofType: Type): Array[_] = {
    ofType match {
      case IntType => Array.fill[Int](size)(0)
      case LongType => Array.fill[Long](size)(0L)
      case DoubleType => Array.fill[Double](size)(0d)
      case BooleanType => Array.fill[Boolean](size)(false)
      case _ => Array.fill[Any](size)(null)
    }
  }

  private[crossbow] def getColumnData(columnName: String): Array[_] = {
    val columnIndex = schema.indexOf(columnName)
    columnData(columnIndex)
  }

  class GroupedView private[DataFrame](keyExprs: Seq[Expr]) {
    def agg(aggExprs: Expr*): DataFrame = GroupBy(DataFrame.this, keyExprs, aggExprs)
  }

  class TypedView[T] private[DataFrame]() extends Iterable[T] {
    private implicit val t2Tuple: Seq[Any] => T = toTuple[T](numColumns)

    def apply(index: Int): T = DataFrame.this (index)

    def apply(range: Range): Seq[T] = for (i <- range) yield this (i)

    override def iterator: Iterator[T] = this (0 until rowCount).iterator

    override def knownSize: Int = rowCount
  }

  override def iterator: Iterator[Seq[Any]] = (for (i <- 0 until rowCount) yield this (i)).iterator

}

object DataFrame {

  def fromSeq[T: ru.TypeTag](data: Seq[T]): DataFrame = {
    if (data.isEmpty) new DataFrame(Vector.empty, Schema())
    else {
      val dataType = Types.toInternalType(ru.typeOf[T])
      dataType match {
        case ProductType(elementTypes@_*) =>
          val tupleData = data.asInstanceOf[Seq[Product]]
          val columnData = elementTypes.zipWithIndex.map({ case (t, i) => convert(tupleData.map(_.productElement(i)), t) })
          val columnSchemas = elementTypes.zipWithIndex.map({ case (t, i) => Column(s"_$i", t) })
          new DataFrame(columnData.toVector, Schema(columnSchemas.toList))
        case _ =>
          val col = convert(data, dataType)
          new DataFrame(Vector(col), Schema(List(new Column("_0", dataType))))
      }
    }
  }

  def fromColumns(columns: IndexedSeq[Seq[Any]], schema: Schema): DataFrame = {
    val columnData = columns.zip(schema.columns).map({ case (data, col) => convert(data, col.columnType) })
    new DataFrame(columnData.toVector, schema)
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
