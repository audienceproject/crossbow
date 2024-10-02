package com.audienceproject.crossbow.core

import com.audienceproject.crossbow.{*, given}
import com.audienceproject.crossbow.exceptions.NoOrderingException
import com.audienceproject.crossbow.expr.Order
import org.scalatest.funsuite.AnyFunSuite

class SortByTest extends AnyFunSuite:

  private val df = DataFrame.fromSeq(Seq(("a", 1), ("b", 2), ("c", 3), ("d", 4))).renameColumns("k", "v")

  test("sortBy on single Double column with implicit ordering"):
    val result = df.sortBy(1.0 / $"v").select($"k").as[String].toSeq
    val expected = Seq("d", "c", "b", "a")
    assert(result == expected)

  test("sortBy on single Float column with implicit ordering"):
    val result = df.sortBy(1.0f / $"v").select($"k").as[String].toSeq
    val expected = Seq("d", "c", "b", "a")
    assert(result == expected)

  test("sortBy on single column with explicit ordering"):
    val result = df.sortBy($"v")(using Order.by(Ordering.Int.reverse)).select($"k").as[String].toSeq
    val expected = Seq("d", "c", "b", "a")
    assert(result == expected)

  test("sortBy on multiple columns with implicit orderings"):
    val result = df.sortBy(($"v" % 2, $"k")).select($"k").as[String].toSeq
    val expected = Seq("b", "d", "a", "c")
    assert(result == expected)

  private case class Custom(x: Int)

  test("No ordering on custom type"):
    val makeCustom = lambda[Int, Custom](Custom.apply)
    val customDf = df.addColumn(makeCustom($"v") as "custom")
    assertThrows[NoOrderingException](customDf.sortBy($"custom"))

  test("sortBy with explicit ordering on custom type"):
    val makeCustom = lambda[Int, Custom](Custom.apply)
    val customDf = df.addColumn(makeCustom($"v") as "custom")
    given customOrdering: Order = Order.by((c1: Custom, c2: Custom) => c1.x - c2.x)
    val result = customDf.sortBy($"custom").select($"k").as[String].toSeq
    val expected = Seq("a", "b", "c", "d")
    assert(result == expected)
