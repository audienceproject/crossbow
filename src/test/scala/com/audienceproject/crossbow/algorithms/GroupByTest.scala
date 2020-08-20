package com.audienceproject.crossbow.algorithms

import com.audienceproject.crossbow.DataFrame
import com.audienceproject.crossbow.Implicits._
import com.audienceproject.crossbow.exceptions.AggregationException
import org.scalatest.funsuite.AnyFunSuite

class GroupByTest extends AnyFunSuite {

  private val df = DataFrame.fromSeq(Seq(
    ("a", 1), ("b", 2), ("c", 3),
    ("a", 4), ("b", 5), ("c", 6)
  )).renameColumns("k", "v")

  test("GroupBy with composite aggregator") {
    val groupedDf = df.groupBy($"k").agg(sum($"v" * 2) / count($"v") as "avg")
    val result = groupedDf.as[(String, Int)].toSeq.sortBy(_._1)
    val expected = Seq(("a", 5), ("b", 7), ("c", 9))
    assert(result == expected)
  }

  test("GroupBy with multiple aggregators") {
    val groupedDf = df.groupBy($"k").agg(sum($"v") as "sum", collect($"v") as "list")
    val result = groupedDf.as[(String, Int, Seq[Int])].toSeq.sortBy(_._1)
    val expected = Seq(("a", 5, Seq(1, 4)), ("b", 7, Seq(2, 5)), ("c", 9, Seq(3, 6)))
    assert(result == expected)
  }

  test("GroupBy on multiple keys") {
    val groupedDf = df.groupBy($"k", $"v" % 1 as "mod").agg(count(1) as "count")
    val expectedNames = Seq("k", "mod", "count")
    val expectedResuls = Seq(("a", 0, 2), ("b", 0, 2), ("c", 0, 2))
    val result = groupedDf.as[(String, Int, Int)].toSeq.sortBy(_._1)
    assert(groupedDf.schema.columns.map(_.name) == expectedNames)
    assert(result == expectedResuls)
  }

  test("GroupBy with custom aggregator function") {
    val product = reducer[Int, Int](1)(_ * _)
    val groupedDf = df.groupBy($"k").agg(product($"v"))
    val result = groupedDf.as[(String, Int)].toSeq.sortBy(_._1)
    val expected = Seq(("a", 4), ("b", 10), ("c", 18))
    assert(result == expected)
  }

  test("GroupBy without any aggregators") {
    assertThrows[AggregationException](df.groupBy($"k").agg($"v"))
  }

  test("Aggregator used without GroupBy") {
    assertThrows[AggregationException](df.select(sum($"k")))
  }

}
