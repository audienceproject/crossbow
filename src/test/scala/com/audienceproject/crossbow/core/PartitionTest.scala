package com.audienceproject.crossbow.core

import com.audienceproject.crossbow.schema.{Column, Schema}
import com.audienceproject.crossbow.{*, given}
import org.scalatest.funsuite.AnyFunSuite

class PartitionTest extends AnyFunSuite:

  private def toSortedIntSeq(df: DataFrame) = df.iterator.toSeq.asInstanceOf[Seq[Int]].sorted

  test("Partition smoke test"):
    val data = 0 until 100
    val schema: Schema = Schema(Seq(Column[Int]("idx")))

    val df = DataFrame.fromColumns(IndexedSeq(data), schema)
    val (selected, notSelected) = df.partition($"idx" <= 50)

    assert(toSortedIntSeq(selected) == data.filter(_ <= 50).sorted)
    assert(toSortedIntSeq(notSelected) == data.filter(_ > 50).sorted)
    assert(toSortedIntSeq(selected.union(notSelected)) == data.sorted)

  test("Partition test, selected no rows"):
    val data = 0 until 100
    val schema: Schema = Schema(Seq(Column[Int]("idx")))

    val df = DataFrame.fromColumns(IndexedSeq(data), schema)
    val (selected, notSelected) = df.partition($"idx" > 100)

    assert(selected.isEmpty)
    assert(toSortedIntSeq(notSelected) == data.sorted)

  test("Partition test, selected all rows"):
    val data = 0 until 100
    val schema: Schema = Schema(Seq(Column[Int]("idx")))

    val df = DataFrame.fromColumns(IndexedSeq(data), schema)
    val (selected, notSelected) = df.partition($"idx" < 100)

    assert(notSelected.isEmpty)
    assert(toSortedIntSeq(selected) == data.sorted)
