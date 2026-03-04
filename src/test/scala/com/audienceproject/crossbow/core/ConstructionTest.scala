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

  test("Construct using builder supports empty columns"):
    val expectedSchema = Schema(Seq(Column[Int]("a"), Column[Float]("b"), Column[String]("c"), Column[Integer]("d")))

    val df = DataFrame.builder()
      .withColumn("a", Array.empty[Int])
      .withColumn("b", Array.empty[Float])
      .withColumn("c", Array.empty[String])
      .withColumn("d", Array.empty[Integer])
      .toDataFrame

    assertResult(4)(df.numColumns)
    assertResult(0)(df.rowCount)
    assertResult(expectedSchema)(df.schema)

  test("Construct using builder produces valid dataframe"):
    val expectedSchema = Schema(Seq(Column[Int]("a"), Column[Float]("b"), Column[String]("c"), Column[Integer]("d")))
    val expectedRows = Seq((1, 0.1f, "a", 10), (2, 0.2f, "b", 20), (3, 0.3f, "c", 30))

    val df = DataFrame.builder()
      .withColumn("a", Array(1, 2, 3))
      .withColumn("b", Array(0.1f, 0.2f, 0.3f))
      .withColumn("c", Array("a", "b", "c"))
      .withColumn("d", Array(10, 20, 30).map(Int.box))
      .toDataFrame

    assertResult(4)(df.numColumns)
    assertResult(3)(df.rowCount)
    assertResult(expectedSchema)(df.schema)
    assertResult(expectedRows)(df.as[(Int, Float, String, Integer)].toSeq)

  test("Construct using builder prevents jagged dataframe"):
    val ex = intercept[IllegalArgumentException]:
      DataFrame.builder().withColumn("a", Array(1, 2, 3)).withColumn("b", Array(0.1f, 0.2f))

    assertResult("Column 'b' must contain 3 rows. Was 2.")(ex.getMessage)
