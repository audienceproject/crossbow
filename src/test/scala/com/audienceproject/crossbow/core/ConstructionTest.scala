package com.audienceproject.crossbow.core

import com.audienceproject.crossbow.*
import com.audienceproject.crossbow.schema.{Column, Schema}
import org.scalatest.funsuite.AnyFunSuite

class ConstructionTest extends AnyFunSuite:

  test("Construct from typed empty Seq"):
    val df = DataFrame.fromSeq(Seq.empty[(Int, Long)])
    assertResult(2)(df.numColumns)
    assertResult(Schema(Seq(Column[Int]("_0"), Column[Long]("_1"))))(df.schema)

  test("Construct using extension method"):
    val df = Seq.empty[(Int, Long)].toDataFrame()
    assertResult(2)(df.numColumns)
    assertResult(Schema(Seq(Column[Int]("_0"), Column[Long]("_1"))))(df.schema)

  test("Construct with names"):
    val df = Seq.empty[(Int, Long)].toDataFrame("a", "b")
    assertResult(2)(df.numColumns)
    assertResult(Schema(Seq(Column[Int]("a"), Column[Long]("b"))))(df.schema)

  test("Construct with floats"):
    val df = Seq.empty[(Int, Float)].toDataFrame("a", "b")
    assertResult(2)(df.numColumns)
    assertResult(Schema(Seq(Column[Int]("a"), Column[Float]("b"))))(df.schema)
