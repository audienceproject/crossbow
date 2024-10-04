package com.audienceproject.crossbow.core

import com.audienceproject.crossbow.{*, given}
import org.scalatest.funsuite.AnyFunSuite

class SelectFilterTest extends AnyFunSuite:

  private val df = DataFrame.fromSeq(Seq(("a", 1, 1.0), ("b", 2, 0.5), ("c", 3, 2.0))).renameColumns("k", "x", "y")

  test("Select named columns"):
    val result = df.select($"k", $"x", $"y" as "newY").schema.columns.map(_.name)
    val expected = Seq("k", "x", "newY")
    assert(result == expected)

  test("Select expressions as new columns"):
    val result = df.select($"x" * $"x" as "square", $"x" / $"y" as "div").as[(Int, Double)].toSeq
    val expected = Seq((1, 1.0), (4, 4.0), (9, 1.5))
    assert(result == expected)

  test("Lambda expressions"):
    val toUpper = lambda[String, String](_.toUpperCase)
    val result = df.select(toUpper($"k") as "upper").as[String].toSeq
    val expected = Seq("A", "B", "C")
    assert(result == expected)

  test("Seq expressions"):
    val result = df.select(seq($"x" * 1.0, $"y")).as[Seq[Double]].toSeq
    val expected = Seq(Seq(1.0, 1.0), Seq(2.0, 0.5), Seq(3.0, 2.0))
    assert(result == expected)

  test("Tuple expressions"):
    val result = df.select(($"k", $"x"), ($"k", $"y")).as[((String, Int), (String, Double))].toSeq
    val expected = Seq((("a", 1), ("a", 1.0)), (("b", 2), ("b", 0.5)), (("c", 3), ("c", 2.0)))
    assert(result == expected)

  test("Filter on calculated expression"):
    val result = df.filter($"x" * $"y" < 2).select($"k").as[String].toSeq
    val expected = Seq("a", "b")
    assert(result == expected)

  test("Filter floats"):
    val df = Seq("a" -> 0.5f, "b" -> 1.0f, "c" -> 1.5f, "d" -> 2.0f).toDataFrame("k", "x")
    val result = df.filter($"x" < 1.1f).select($"k").as[String].toSeq
    val expected = Seq("a", "b")
    assert(result == expected)
