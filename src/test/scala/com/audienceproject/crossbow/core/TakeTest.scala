package com.audienceproject.crossbow.core

import com.audienceproject.crossbow.*
import org.scalatest.funsuite.AnyFunSuite

class TakeTest extends AnyFunSuite:

  test("Construct from typed empty Seq"):
    val data = Seq.tabulate(100)(i => (i, i * 2.0))
    val df = DataFrame.fromSeq(data)
    assertResult(data.take(10))(df.take(10).as[(Int, Double)].toSeq)
