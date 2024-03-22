package com.audienceproject.crossbow.algorithms

import com.audienceproject.crossbow.DataFrame
import com.audienceproject.crossbow.exceptions.AggregationException
import com.audienceproject.crossbow.expr.Expr
import com.audienceproject.crossbow.schema.{Column, Schema}

import scala.collection.mutable

private[crossbow] object GroupBy {

  def apply(dataFrame: DataFrame, keyExprs: Seq[Expr], aggExprs: Seq[Expr]): DataFrame = {
    val aggregators = aggExprs.toList.flatMap:
      Traversal.collect(_):
        case agg: Expr.Aggregate[?, ?] => agg
        case col: Expr.Cell => throw new AggregationException(col)

    val keyEvals = keyExprs.map(_.eval).toList
    val reducers = aggregators.map(_.reduce.asInstanceOf[(Any, Any) => Any])

    val groups = mutable.HashMap.empty[List[Any], List[Any]].withDefaultValue(aggregators.map(_.seed))
    for (i <- 0 until dataFrame.rowCount) {
      val keys = keyEvals.map(_(i))
      val values = groups(keys)
      groups.put(keys, reducers.zip(values).map({ case (reducer, value) => reducer(i, value) }))
    }

    val (orderedKeys, orderedResult) = groups.toSeq.unzip
    val newKeyCols = Vector.tabulate(keyExprs.size)(i => orderedKeys.map(_(i)))
    val newDataCols = Vector.tabulate(reducers.size)(i => orderedResult.map(_(i)))

    val keyNames = keyExprs.zipWithIndex.map({
      case (Expr.Named(name, _), _) => name
      case (Expr.Cell(name), _) => name
      case (_, i) => s"_key$i"
    })
    val keySchemaCols = keyNames.zip(keyExprs).map({ case (name, expr) => Column(name, expr.typeOf) }).toList
    val dataSchemaCols = aggregators.zipWithIndex.map({ case (expr, i) => Column(s"_res${i + 1}", expr.typeOf) })

    val keySelectExprs: Seq[DataFrame ?=> Expr] = keySchemaCols.map(c => Expr.Cell(c.name))
    var index = 0
    val valueSelectExprs: Seq[DataFrame ?=> Expr] = aggExprs.map:
      Traversal.transform(_):
        case agg: Expr.Aggregate[?, ?] =>
          index = index + 1
          Expr.Cell(s"_res$index")

    val temp = DataFrame.fromColumns(newKeyCols ++ newDataCols, Schema(keySchemaCols ++ dataSchemaCols))
    temp.select(keySelectExprs ++ valueSelectExprs *)
  }

}
