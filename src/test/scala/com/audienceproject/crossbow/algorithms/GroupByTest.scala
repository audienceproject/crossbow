package com.audienceproject.crossbow.algorithms

import com.audienceproject.crossbow.DataFrame
import com.audienceproject.crossbow.Implicits._
import org.scalatest.funsuite.AnyFunSuite

class GroupByTest extends AnyFunSuite {

  private val df = DataFrame.fromSeq(Seq(
    ("a", 1), ("b", 2), ("c", 3),
    ("a", 4), ("b", 5), ("c", 6)
  )).renameColumns("k", "v")

  test("GroupBy with composite aggregator") {
    val groupedDf = df.groupBy($"k").agg(sum($"v" * 2) / count($"v") as "avg")
    val result = groupedDf.as[(String, Int)].toSeq
    val expected = Seq(("a", 5), ("b", 7), ("c", 9))
    assert(result == expected)
  }

  test("GroupBy with multiple aggregators") {
    val groupedDf = df.groupBy($"k").agg(sum($"v") as "sum", collect($"v") as "list")
    val result = groupedDf.as[(String, Int, Seq[Int])].toSeq
    val expected = Seq(("a", 5, Seq(1, 4)), ("b", 7, Seq(2, 5)), ("c", 9, Seq(3, 6)))
    assert(result == expected)
  }

}
