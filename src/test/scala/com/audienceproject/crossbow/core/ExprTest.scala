package com.audienceproject.crossbow.core

import com.audienceproject.crossbow.*
import org.scalatest.funsuite.AnyFunSuite

class ExprTest extends AnyFunSuite:

  test("Typecheck generic lambda"):
    val df = DataFrame.fromSeq(Seq(("a", Some(1)), ("b", None), ("c", Some(2))))
    val getOrElse = lambda[Option[Int], Int](_.getOrElse(0))
    val result = df.select(getOrElse($"_1")).as[Int].toSeq
    assertResult(Seq(1, 0, 2))(result)

    val df2 = DataFrame.fromSeq(Seq(List(Some(1)), List(None), List(Some(2))))
    val unwrap = lambda[List[Option[Int]], Int](_.head.getOrElse(0))
    val result2 = df2.select(unwrap($"_0")).as[Int].toSeq
    assertResult(Seq(1, 0, 2))(result)

    val unwrapInvalidType = lambda[List[Some[Int]], Int](_.head.get)
    assertThrows[ClassCastException](df2.select(unwrapInvalidType($"_0")).as[Int].toSeq)
