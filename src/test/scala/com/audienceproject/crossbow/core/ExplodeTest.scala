package com.audienceproject.crossbow.core

import com.audienceproject.crossbow.{*, given}
import org.scalatest.funsuite.AnyFunSuite

class ExplodeTest extends AnyFunSuite:

  test("Explode int seq column"):
    val df = DataFrame.fromSeq(Seq(("a", Seq(1, 2)), ("b", Seq(1, 2, 3)), ("c", Seq(5, 10))))
    val result = df.explode($"_1")
    val expected = Seq(("a", 1), ("a", 2), ("b", 1), ("b", 2), ("b", 3), ("c", 5), ("c", 10))
    assert(result("_0", "_2").as[(String, Int)].toSeq == expected)

  test("Explode float seq column"):
    val df = DataFrame.fromSeq(Seq(("a", Seq(1F, 2F)), ("b", Seq(1F, 2F, 3F)), ("c", Seq(5F, 10F))))
    val result = df.explode($"_1")
    val expected = Seq(("a", 1F), ("a", 2F), ("b", 1F), ("b", 2F), ("b", 3F), ("c", 5F), ("c", 10F))
    assert(result("_0", "_2").as[(String, Float)].toSeq == expected)

  test("Explode on evaluated expression"):
    val df = DataFrame.fromSeq(Seq(("a", 2), ("b", 3), ("c", 4)))
    val range = lambda[(Int, Int), Seq[Int]] { case (from, to) => from until to }
    val result = df.explode(range((index(), $"_1")))
    val expected = Seq(("a", 0), ("a", 1), ("b", 1), ("b", 2), ("c", 2), ("c", 3))
    assert(result("_0", "_2").as[(String, Int)].toSeq == expected)

  test("Explode on object column"):
    val df = DataFrame.fromSeq(Seq(("a", Seq(Left(1), Right(2))), ("b", Seq(Left(2), Right(3)))))
    val result = df.explode($"_1" as "either")
    val expected = Seq(("a", Left(1)), ("a", Right(2)), ("b", Left(2)), ("b", Right(3)))
    assert(result("_0", "either").as[(String, Any)].toSeq == expected)
