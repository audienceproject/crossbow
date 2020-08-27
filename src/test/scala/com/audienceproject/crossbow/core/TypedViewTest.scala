package com.audienceproject.crossbow.core

import com.audienceproject.crossbow.DataFrame
import com.audienceproject.crossbow.Implicits._
import org.scalatest.funsuite.AnyFunSuite

class TypedViewTest extends AnyFunSuite {

  private val df = DataFrame.fromSeq(Seq(("a", 1), ("b", 2), ("c", 3)))

  test("Cast DataFrame to TypedView of single type") {
    val result = df.select($"_0").as[String].toSeq
    val expected = Seq("a", "b", "c")
    assert(result == expected)
  }

  test("Cast DataFrame to TypedView of tuple") {
    val result = df.as[(String, Int)].toSeq
    val expected = Seq(("a", 1), ("b", 2), ("c", 3))
    assert(result == expected)
  }

}
