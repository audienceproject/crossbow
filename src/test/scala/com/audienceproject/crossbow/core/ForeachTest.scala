package com.audienceproject.crossbow.core

import com.audienceproject.crossbow.*
import org.scalatest.funsuite.AnyFunSuite

class ForeachTest extends AnyFunSuite:

  test("Apply function to all rows of DataFrame"):
    val data = Seq.tabulate(100)(i => (i, i * 2.0))
    val df = DataFrame.fromSeq(data)
    var sumInts = 0
    var sumDoubles = 0.0
    df.foreach: row =>
      sumInts = sumInts + row[Int](0)
      sumDoubles = sumDoubles + row[Double](1)
    assertResult(99 * 100 / 2)(sumInts)
    assertResult(99 * 100.0)(sumDoubles)
