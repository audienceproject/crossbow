package com.audienceproject.crossbow.algorithms

import com.audienceproject.crossbow.expr.{Aggregator, Expr}
import com.audienceproject.crossbow.schema.{Column, Schema}
import com.audienceproject.crossbow.{DataFrame, schema}

import scala.collection.mutable

private[crossbow] object GroupBy {

  def apply(dataFrame: DataFrame, expr: Expr, aggregators: Seq[Aggregator]): (List[Array[Any]], Schema) = {
    val keyEval = expr.compile(dataFrame)
    val reducers = aggregators.toList.map(_.compile(dataFrame))
    val groups = mutable.HashMap.empty[Any, List[Any]].withDefaultValue(reducers.map(_.seed))
    for (i <- 0 until dataFrame.rowCount) {
      val key = keyEval(i)
      val values = groups(key)
      groups.put(key, reducers.zip(values).map({ case (reducer, value) => reducer(i, value) }))
    }

    val (orderedKeys, orderedResult) = groups.toArray.unzip
    val newCols = orderedKeys :: List.tabulate(aggregators.size)(i => orderedResult.map(_ (i)))
    val newSchemaCols = reducers.zipWithIndex.map({ case (reducer, i) => Column(s"_$i", reducer.typeOf) })
    val newSchema = schema.Schema(Column("key", keyEval.typeOf) :: newSchemaCols)

    (newCols, newSchema)
  }

}
