package com.audienceproject.crossbow.core

import com.audienceproject.crossbow.*
import org.scalatest.funsuite.AnyFunSuite

class UnionTest extends AnyFunSuite:

  test("Union two identical frames"):
    val df = DataFrame.fromSeq(Seq(("a", 1), ("b", 2), ("c", 3)))
    val result = df.union(df).as[(String, Int)].toSeq
    val expected = df.as[(String, Int)].toSeq ++ df.as[(String, Int)].toSeq
    assert(result == expected)

  test("Union frames with different schemas"):
    val left = DataFrame.fromSeq(Seq(("a", 1), ("b", 2), ("c", 3))).renameColumns("x", "y")
    val right = DataFrame.fromSeq(Seq((1, "d", 10), (2, "e", 11))).renameColumns("y", "x", "z")
    val result = left.union(right).as[(String, Int, Int)].toSeq
    val expected = Seq(("a", 1, 0), ("b", 2, 0), ("c", 3, 0), ("d", 1, 10), ("e", 2, 11))
    assert(result == expected)

  test("Union frames with incompatible schemas"):
    val left = DataFrame.fromSeq(Seq(1, 2, 3))
    val right = DataFrame.fromSeq(Seq(1d, 2d))
    assertThrows[IllegalArgumentException](left.union(right))

  test("Union all"):
    val df = DataFrame.fromSeq(Seq(("a", 1), ("b", 2), ("c", 3)))
    val result = DataFrame.unionAll(Seq(df, df, df)).as[(String, Int)].toSeq
    val expected = df.union(df).union(df).as[(String, Int)].toSeq
    assert(result == expected)
