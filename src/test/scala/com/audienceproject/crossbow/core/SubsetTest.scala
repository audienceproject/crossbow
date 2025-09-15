package com.audienceproject.crossbow.core

import com.audienceproject.crossbow.DataFrame
import com.audienceproject.crossbow.schema.{Column, Schema}
import org.scalatest.funsuite.AnyFunSuite

class SubsetTest extends AnyFunSuite:

  private val dataframe = DataFrame.fromColumns(IndexedSeq(0 until 100), Schema(Seq(Column[Int]("idx"))))

  test("Retrieve single row by index"):
    assertResult(0)(dataframe(0))
    assertResult(10)(dataframe(10))
    assertResult(45)(dataframe(45))
    assertResult(99)(dataframe(99))

  test("Retrieve contiguous range"):
    val range = 33 to 38
    val subset = dataframe(range)

    assert(range.iterator.sameElements(subset.iterator))

  test("Retrieve non-contiguous range"):
    val indices = IndexedSeq(5, 16, 27, 38, 49, 60, 71, 82, 93)
    val subset = dataframe(indices)

    assert(indices.iterator.sameElements(subset.iterator))
