package com.audienceproject.crossbow

import com.audienceproject.crossbow.algorithms.{GroupBy, SortMergeJoin}
import com.audienceproject.crossbow.exceptions.{IncorrectTypeException, JoinException}
import com.audienceproject.crossbow.expr._
import com.audienceproject.crossbow.schema.{Column, Schema}

import scala.util.Sorting

class DataFrame private(private val columnData: Vector[Array[_]], val schema: Schema,
                        private val sortKey: Option[Expr] = None) {

  val rowCount: Int = if (columnData.isEmpty) 0 else columnData.head.length
  val numColumns: Int = columnData.size

  /**
   * Retrieve a single row by index.
   *
   * @param index row index
   * @return row as a sequence of values
   */
  def apply(index: Int): Seq[Any] =
    if (isEmpty) Seq.empty
    else columnData.map(_ (index))

  /**
   * Retrieve a subset of rows from this DataFrame based on range of indices.
   *
   * @param range range of row indices to retrieve
   * @return new DataFrame
   */
  def apply(range: Range): DataFrame = slice(range, sortKey)

  /**
   * Select a subset of columns from this DataFrame.
   *
   * @param columnNames names of columns to select
   * @return new DataFrame
   */
  def apply(columnNames: String*): DataFrame = {
    val colExprs = columnNames.map(Expr.Cell)
    select(colExprs: _*)
  }

  /**
   * Typecast this DataFrame to a TypedView of the type parameter 'T'. All columns in this DataFrame will have to be
   * accounted for in the given type. A DataFrame with multiple columns will have its rows represented as tuples
   * of the individual types of these columns.
   *
   * @tparam T the type of a row in this DataFrame
   * @return [[TypedView]] on the contents of this DataFrame
   */
  def as[T: ru.TypeTag]: TypedView[T] = {
    val dataType = ru.typeOf[T]
    schema.columns match {
      case Seq() => throw new IncorrectTypeException(dataType, AnyType(ru.typeOf[Nothing]))
      case Seq(col) =>
        if (Types.typecheck(col.columnType, dataType))
          new TypedColumnView[T](getColumnData(col.name).asInstanceOf[Array[T]])
        else throw new IncorrectTypeException(dataType, col.columnType)
      case cols =>
        val schemaType = ProductType(cols.map(_.columnType): _*)
        if (Types.typecheck(schemaType, dataType)) new TypedView[T]
        else throw new IncorrectTypeException(dataType, schemaType)
    }
  }

  /**
   * Add a column to the DataFrame, evaluating to 'expr' at each individual row index.
   * Use the 'as' method on [[Expr]] to give the column a name.
   *
   * @param expr the [[Expr]] to evaluate as the new column
   * @return new DataFrame
   */
  def addColumn(expr: Expr): DataFrame = {
    val eval = expr.compile(this)
    val newCol = sliceColumn(eval, 0 until rowCount)
    val newColSchema = expr match {
      case Expr.Named(columnName, _) => Column(columnName, eval.typeOf)
      case _ => Column(s"_$numColumns", eval.typeOf)
    }
    new DataFrame(columnData :+ newCol, schema.add(newColSchema))
  }

  /**
   * Remove one or more columns from the DataFrame.
   *
   * @param columnNames the names of the columns to remove
   * @return new DataFrame
   */
  def removeColumns(columnNames: String*): DataFrame = {
    val remainingIndices = for ((c, i) <- schema.columns.zipWithIndex if !columnNames.contains(c.name)) yield i
    val colData = remainingIndices.map(columnData)
    new DataFrame(colData.toVector, Schema(remainingIndices.map(schema.columns)))
  }

  /**
   * Map over this DataFrame, selecting a set of expressions which will become the columns of a new DataFrame.
   * Use the 'as' method on [[Expr]] to give names to the new columns. An expression which is only a column accessor
   * will inherit the accessed column's name (unless it is renamed).
   *
   * @param exprs the list of [[Expr]] to evaluate as a new DataFrame
   * @return new DataFrame
   */
  def select(exprs: Expr*): DataFrame = {
    val (colData, colSchemas) = exprs.zipWithIndex.map({
      case (Expr.Named(newName, Expr.Cell(colName)), _) => (getColumnData(colName), schema.get(colName).renamed(newName))
      case (Expr.Cell(colName), _) => (getColumnData(colName), schema.get(colName))
      case (expr, i) =>
        val eval = expr.compile(this)
        val newColSchema = expr match {
          case Expr.Named(columnName, _) => new Column(columnName, eval.typeOf)
          case _ => new Column(s"_$i", eval.typeOf)
        }
        (sliceColumn(eval, 0 until rowCount), newColSchema)
    }).unzip[Array[_], Column](p => (p._1, p._2))
    new DataFrame(colData.toVector, Schema(colSchemas))
  }

  /**
   * Retrieve a subset of rows from this DataFrame based on the boolean evaluation of the given expression.
   *
   * @param expr the [[Expr]] to evaluate, if 'true' the given row will appear in the output
   * @return new DataFrame
   */
  def filter(expr: Expr): DataFrame = {
    val eval = expr.compile(this).typecheckAs[Boolean]
    val indices = for (i <- 0 until rowCount if eval(i)) yield i
    slice(indices, sortKey)
  }

  /**
   * Explode this DataFrame on the given expression, flattening its contents and repeating all other cells on the
   * row for every element in the sequence. The given [[Expr]] must evaluate to a list type.
   * Use the 'as' method on [[Expr]] to name the flattened column.
   *
   * @param expr the [[Expr]] to explode on
   * @return new DataFrame
   */
  def explode(expr: Expr): DataFrame = {
    val eval = expr.compile(this).typecheckAs[Seq[_]]
    val ListType(innerType) = eval.typeOf // This unapply is safe due to typecheck.
    val nestedCol = fillArray[Seq[_]](0 until rowCount, eval.apply)
    val reps = nestedCol.map(_.size)
    val colData = for (i <- 0 until numColumns) yield repeatColumn(columnData(i), schema.columns(i).columnType, reps)
    val explodedColSchema = expr match {
      case Expr.Named(newName, _) => Column(newName, innerType)
      case _ => Column(s"_$numColumns", innerType)
    }
    val explodedData = convert(nestedCol.toSeq.flatten, innerType)
    new DataFrame(colData.toVector :+ explodedData, Schema(schema.columns :+ explodedColSchema))
  }

  /**
   * Partition this DataFrame into groups, defined by the given set of expressions.
   * The evaluation of each of the 'keyExprs' will appear as a column in the output.
   *
   * @param keyExprs the list of [[com.audienceproject.crossbow.expr.Expr]] that will evaluate to the keys of the groups
   * @return [[GroupedView]] on this DataFrame
   */
  def groupBy(keyExprs: Expr*): GroupedView = new GroupedView(keyExprs)

  /**
   * Sort this DataFrame by the evaluation of 'expr'. If a natural ordering exists on this value, it will be used.
   * User-defined orderings on other types or for overwriting the natural orderings with an explicit ordering can be
   * supplied through the 'givenOrderings' argument.
   *
   * @param expr           the [[Expr]] to evaluate as a sort key
   * @param givenOrderings explicit [[Order]] to use on the sort key, or list of [[Order]] if the key is a tuple
   * @return new DataFrame
   */
  def sortBy(expr: Expr, givenOrderings: Order*): DataFrame = {
    if (sortKey.contains(expr) && givenOrderings.isEmpty) this
    else {
      val eval = expr.compile(this)
      val ord = Order.getOrdering(eval.typeOf, givenOrderings)
      val indices = Array.tabulate(rowCount)(identity)
      Sorting.quickSort[Int](indices)(new Ordering[Int] {
        override def compare(x: Int, y: Int): Int = ord.compare(eval(x), eval(y))
      })
      slice(indices.toIndexedSeq, if (givenOrderings.isEmpty) Some(expr) else None)
    }
  }

  /**
   * Join this DataFrame on another DataFrame, with the key evaluated by 'joinExpr'.
   * The resulting DataFrame will contain all the columns of this DataFrame and the other, where the column names of
   * the other will be prepended with "#".
   *
   * @note 'joinExpr' must evaluate to a type with a natural ordering
   * @param other    DataFrame to join with this one
   * @param joinExpr [[Expr]] to evaluate as join key
   * @param joinType [[JoinType]] as one of Inner, FullOuter, LeftOuter or RightOuter
   * @return new DataFrame
   */
  def join(other: DataFrame, joinExpr: Expr, joinType: JoinType = JoinType.Inner): DataFrame = {
    val internalType = joinExpr.compile(this).typeOf
    if (internalType != joinExpr.compile(other).typeOf) throw new JoinException(joinExpr)
    val ordering = Order.getOrdering(internalType)
    SortMergeJoin(this.sortBy(joinExpr), other.sortBy(joinExpr), joinExpr, joinType, ordering)
  }

  /**
   * Union this DataFrame with another DataFrame. Columns will be matched by name, and if matched they must have the
   * same type. Columns that are not present in one or the other DataFrame will contain null-values in the output
   * for the rows of the DataFrame in which the column was not present.
   *
   * @param other DataFrame to union with this one
   * @return new DataFrame
   */
  def union(other: DataFrame): DataFrame = {
    if (schema == other.schema) {
      val colData = for (i <- 0 until numColumns) yield {
        val combinedData = Seq(columnData(i), other.columnData(i))
        spliceColumns(combinedData, schema.columns(i).columnType)
      }
      new DataFrame(colData.toVector, schema)
    } else {
      val matchingColumns = schema.matchColumns(other.schema)
      val colData = matchingColumns.map({
        case (c, Left((i, j))) => spliceColumns(Seq(columnData(i), other.columnData(j)), c.columnType)
        case (c, Right(Left(i))) => padColumn(columnData(i), c.columnType, padding = -other.rowCount)
        case (c, Right(Right(j))) => padColumn(other.columnData(j), c.columnType, padding = rowCount)
      })
      new DataFrame(colData.toVector, Schema(matchingColumns.map(_._1)))
    }
  }

  /**
   * Rename the columns of this DataFrame.
   *
   * @param newNames list of new names for each column of this DataFrame
   * @return new DataFrame
   */
  def renameColumns(newNames: String*): DataFrame = {
    if (newNames.size != numColumns) throw new IllegalArgumentException("Wrong number of column names given.")
    val columnSchemas = schema.columns.zip(newNames).map({ case (col, name) => col.renamed(name) })
    new DataFrame(columnData, Schema(columnSchemas))
  }

  /**
   * Rename the columns of this DataFrame by applying the given function.
   *
   * @param toNewName function to map over the names of the columns
   * @return new DataFrame
   */
  def renameColumns(toNewName: String => String): DataFrame = {
    val columnSchemas = schema.columns.map(col => col.renamed(toNewName(col.name)))
    new DataFrame(columnData, Schema(columnSchemas))
  }

  def printSchema(): Unit = println(schema)

  private[crossbow] def merge(other: DataFrame): DataFrame = {
    val otherColumns = other.schema.columns.map(col => col.renamed("#" + col.name))
    new DataFrame(columnData ++ other.columnData, Schema(schema.columns ++ otherColumns))
  }

  private[crossbow] def slice(indices: IndexedSeq[Int], slicedSortKey: Option[Expr] = None): DataFrame = {
    val newData = schema.columns.map(col => {
      val eval = Expr.Cell(col.name).compile(this)
      sliceColumn(eval, indices)
    })
    new DataFrame(newData.toVector, schema, slicedSortKey)
  }

  private[crossbow] def getColumnData(columnName: String): Array[_] = {
    val columnIndex = schema.indexOf(columnName)
    columnData(columnIndex)
  }

  class GroupedView private[DataFrame](keyExprs: Seq[Expr]) {
    /**
     * Aggregate this GroupedView to a new DataFrame, evaluated by the list of aggregation expressions.
     * An aggregation expression can be any expression, but it must contain [[Aggregator]] expressions instead
     * of column accessors. The [[Aggregator]] expressions themselves may contain arbitrary expressions with
     * combinations of column accessors.
     * Use the 'as' method on [[Expr]] to name the resulting columns.
     *
     * @param aggExprs the list of [[Expr]] used to aggregate the values of the groups
     * @return new DataFrame
     */
    def agg(aggExprs: Expr*): DataFrame = GroupBy(DataFrame.this, keyExprs, aggExprs)
  }

  class TypedView[T] private[DataFrame]() extends Iterable[T] {
    private implicit val t2Tuple: Seq[Any] => T = toTuple[T](numColumns)

    /**
     * Retrieve a single row by index.
     *
     * @param index row index
     * @return row as a value of type 'T'
     */
    def apply(index: Int): T = DataFrame.this (index)

    /**
     * Retrieve a subset of rows from this view based on range of indices.
     *
     * @param range range of row indices to retrieve
     * @return sequence of rows of type 'T'
     */
    def apply(range: Range): Seq[T] = for (i <- range) yield this (i)

    override def iterator: Iterator[T] = this (0 until rowCount).iterator

    override def size: Int = rowCount
  }

  private class TypedColumnView[T] private[DataFrame](columnData: Array[T]) extends TypedView[T] {
    override def apply(index: Int): T = columnData(index)
  }

  def isEmpty: Boolean = rowCount == 0

  def iterator: Iterator[Seq[Any]] = new Iterator[Seq[Any]] {
    private var pos = -1

    override def hasNext: Boolean = pos < rowCount - 1

    override def next(): Seq[Any] = {
      pos += 1
      DataFrame.this (pos)
    }
  }

}

object DataFrame {

  def empty(): DataFrame = new DataFrame(Vector.empty, Schema())

  /**
   * Construct a new DataFrame from a sequence of rows.
   *
   * @param data the sequence of rows which will form the new DataFrame
   * @tparam T the type of a row, if this is a [[Product]] type each element will become a separate column
   * @return new DataFrame
   */
  def fromSeq[T: ru.TypeTag](data: Seq[T]): DataFrame = {
    if (data.isEmpty) empty()
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

  /**
   * Construct a new DataFrame from a list of columns and a schema.
   *
   * @param columns the columns of the new DataFrame
   * @param schema  the [[Schema]] of the new DataFrame, where each entry matches with the data in the 'columns' list
   * @return new DataFrame
   */
  def fromColumns(columns: IndexedSeq[Seq[Any]], schema: Schema): DataFrame = {
    if (columns.size != schema.size)
      throw new IllegalArgumentException(s"Number of columns is ${columns.size}, but schema is ${schema.size}")
    if (columns.exists(_.size != columns.head.size))
      throw new IllegalArgumentException("All columns must have the same number of rows.")
    val columnData = columns.zip(schema.columns).map({ case (data, col) => convert(data, col.columnType) })
    new DataFrame(columnData.toVector, schema)
  }

  /**
   * Unions all into a single DataFrame. All DataFrames must have the same schema.
   *
   * @param dataFrames list of DataFrame to union
   * @return new DataFrame
   */
  def unionAll(dataFrames: Seq[DataFrame]): DataFrame = {
    if (dataFrames.isEmpty) empty()
    else {
      val schema = dataFrames.head.schema
      if (!dataFrames.tail.forall(_.schema == schema))
        throw new IllegalArgumentException("All DataFrames in unionAll must have the same schema.")
      val colData = for (i <- 0 until schema.size) yield {
        val combinedData = dataFrames.map(_.columnData(i))
        spliceColumns(combinedData, schema.columns(i).columnType)
      }
      new DataFrame(colData.toVector, schema)
    }
  }

}
