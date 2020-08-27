package com.audienceproject.crossbow.algorithms

import com.audienceproject.crossbow.DataFrame
import com.audienceproject.crossbow.exceptions.AggregationException
import com.audienceproject.crossbow.expr.{Aggregator, Expr}
import com.audienceproject.crossbow.schema.{Column, Schema}

import scala.collection.mutable

private[crossbow] object GroupBy {

  def apply(dataFrame: DataFrame, keyExprs: Seq[Expr], aggExprs: Seq[Expr]): DataFrame = {
    val aggregators = mutable.ListBuffer.empty[Aggregator]
    val selectExprs = aggExprs.map(Traversal.transform(_, {
      case agg: Aggregator =>
        aggregators += agg
        Expr.Cell(s"_res${aggregators.size}")
      case col: Expr.Cell => throw new AggregationException(col)
    }))

    val keyEvals = keyExprs.map(_.compile(dataFrame)).toList
    val reducers = aggregators.toList.map(_.reduceOn(dataFrame))

    val groups = mutable.HashMap.empty[List[Any], List[Any]].withDefaultValue(reducers.map(_.seed))
    for (i <- 0 until dataFrame.rowCount) {
      val keys = keyEvals.map(_ (i))
      val values = groups(keys)
      groups.put(keys, reducers.zip(values).map({ case (reducer, value) => reducer(i, value) }))
    }

    val (orderedKeys, orderedResult) = groups.toSeq.unzip
    val newKeyCols = Vector.tabulate(keyExprs.size)(i => orderedKeys.map(_ (i)))
    val newDataCols = Vector.tabulate(reducers.size)(i => orderedResult.map(_ (i)))

    val keyNames = keyExprs.zipWithIndex.map({
      case (Expr.Named(name, _), _) => name
      case (Expr.Cell(name), _) => name
      case (_, i) => s"_key$i"
    })
    val keySchemaCols = keyNames.zip(keyEvals).map({ case (name, eval) => Column(name, eval.typeOf) }).toList
    val dataSchemaCols = reducers.zipWithIndex.map({ case (reducer, i) => Column(s"_res${i + 1}", reducer.typeOf) })

    val temp = DataFrame.fromColumns(newKeyCols ++ newDataCols, Schema(keySchemaCols ++ dataSchemaCols))
    temp.select(keySchemaCols.map(c => Expr.Cell(c.name)) ++ selectExprs: _*)
  }

}
