package com.audienceproject.crossbow.algorithms

import com.audienceproject.crossbow.DataFrame
import com.audienceproject.crossbow.expr.{Aggregator, Expr}
import com.audienceproject.crossbow.schema.{Column, Schema}

import scala.collection.mutable

private[crossbow] object GroupBy {

  def apply(dataFrame: DataFrame, expr: Expr, aggExprs: Seq[Expr]): DataFrame = {
    val aggregators = mutable.ListBuffer.empty[Aggregator]
    val selectExprs = aggExprs.map(Traversal.transform(_, {
      case agg: Aggregator =>
        aggregators += agg
        Expr.Column(s"_${aggregators.size}")
    }))

    val keyEval = expr.compile(dataFrame)
    val reducers = aggregators.toList.map(_.reduceOn(dataFrame))

    val groups = mutable.HashMap.empty[Any, List[Any]].withDefaultValue(reducers.map(_.seed))
    for (i <- 0 until dataFrame.rowCount) {
      val key = keyEval(i)
      val values = groups(key)
      groups.put(key, reducers.zip(values).map({ case (reducer, value) => reducer(i, value) }))
    }

    val (orderedKeys, orderedResult) = groups.toSeq.unzip
    val newCols = orderedKeys :: List.tabulate(aggregators.size)(i => orderedResult.map(_ (i)))
    val newSchemaCols = reducers.zipWithIndex.map({ case (reducer, i) => Column(s"_${i + 1}", reducer.typeOf) })
    val newSchema = Schema(Column("key", keyEval.typeOf) :: newSchemaCols)

    val temp = DataFrame.fromColumns(newCols, newSchema)
    temp.select(Expr.Column("key") +: selectExprs: _*)
  }

}
