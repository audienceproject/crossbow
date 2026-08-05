package com.audienceproject.crossbow.core

import com.audienceproject.crossbow.*
import org.scalatest.funsuite.AnyFunSuite

class TakeTest extends AnyFunSuite:

  test("Take rows from DataFrame"):
    val data = Seq.tabulate(100)(i => (i, i * 2.0))
    val df = DataFrame.fromSeq(data)
    assertResult(data.take(10))(df.take(10).as[(Int, Double)].toSeq)
