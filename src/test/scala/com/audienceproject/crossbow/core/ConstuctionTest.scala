package com.audienceproject.crossbow.core

import com.audienceproject.crossbow.{DataFrame, expr}
import com.audienceproject.crossbow.schema.{Column, Schema}
import org.scalatest.funsuite.AnyFunSuite

class ConstuctionTest extends AnyFunSuite {
  test("construct from typed empty seq") {
    val df = DataFrame.fromSeq(Seq.empty[(Int,Long)])
    assertResult(2)(df.numColumns)
    assertResult(Schema(Seq(Column("_0",expr.IntType),Column("_1",expr.LongType))))(df.schema)
  }
}
