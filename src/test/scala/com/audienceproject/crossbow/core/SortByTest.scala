package com.audienceproject.crossbow.core

import com.audienceproject.crossbow.DataFrame
import com.audienceproject.crossbow.Implicits._
import com.audienceproject.crossbow.exceptions.NoOrderingException
import com.audienceproject.crossbow.expr.Order
import org.scalatest.funsuite.AnyFunSuite

class SortByTest extends AnyFunSuite {

  private val df = DataFrame.fromSeq(Seq(("a", 1), ("b", 2), ("c", 3), ("d", 4))).renameColumns("k", "v")

  test("sortBy on single column with implicit ordering") {
    val result = df.sortBy(1.0 / $"v").select($"k").as[String].toSeq
    val expected = Seq("d", "c", "b", "a")
    assert(result == expected)
  }

  test("sortBy on single column with explicit ordering") {
    val result = df.sortBy($"v", Order.by(Ordering.Int.reverse)).select($"k").as[String].toSeq
    val expected = Seq("d", "c", "b", "a")
    assert(result == expected)
  }

  test("sortBy on multiple columns with implicit orderings") {
    val result = df.sortBy(($"v" % 2, $"k")).select($"k").as[String].toSeq
    val expected = Seq("b", "d", "a", "c")
    assert(result == expected)
  }

  case class Custom(x: Int)

  test("sortBy with explicit ordering on custom type") {
    val makeCustom = lambda[Int, Custom](Custom)
    val customDf = df.addColumn(makeCustom($"v") as "custom")

    assertThrows[NoOrderingException](customDf.sortBy($"custom"))

    val result = customDf.sortBy($"custom", Order.by[Custom]((c1, c2) => c1.x - c2.x)).select($"k").as[String].toSeq
    val expected = Seq("a", "b", "c", "d")
    assert(result == expected)
  }

}
