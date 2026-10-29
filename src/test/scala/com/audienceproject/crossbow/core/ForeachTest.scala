package com.audienceproject.crossbow.core

import com.audienceproject.crossbow.*
import com.audienceproject.crossbow.exceptions.IncorrectTypeException
import org.scalatest.funsuite.AnyFunSuite

class ForeachTest extends AnyFunSuite:

  test("Apply function to all rows of DataFrame"):
    val data = Seq.tabulate(100)(i => (i, i * 2.0))
    val df = DataFrame.fromSeq(data).renameColumns("int", "double")
    val ints = df.column[Int]("int")
    val doubles = df.column[Double]("double")
    var sumInts = 0
    var sumDoubles = 0.0
    df.foreach: row =>
      sumInts = sumInts + ints(row)
      sumDoubles = sumDoubles + doubles(row)
    assertResult(99 * 100 / 2)(sumInts)
    assertResult(99 * 100.0)(sumDoubles)

  test("Read columns of all types"):
    val data = Seq((1, 2L, 3.0, 4.0f, true, "a", Seq(1, 2)), (5, 6L, 7.0, 8.0f, false, "b", Seq(3)))
    val df = DataFrame.fromSeq(data).renameColumns("i", "l", "d", "f", "b", "s", "seq")
    val i = df.column[Int]("i")
    val l = df.column[Long]("l")
    val d = df.column[Double]("d")
    val f = df.column[Float]("f")
    val b = df.column[Boolean]("b")
    val s = df.column[String]("s")
    val seq = df.column[Seq[Int]]("seq")
    val result = Seq.newBuilder[(Int, Long, Double, Float, Boolean, String, Seq[Int])]
    df.foreach(row => result += ((i(row), l(row), d(row), f(row), b(row), s(row), seq(row))))
    assertResult(data)(result.result())

  test("Primitive columns resolve to unboxed accessors"):
    val df = DataFrame.fromSeq(Seq((1, 2L, 3.0, 4.0f, true, "a"))).renameColumns("i", "l", "d", "f", "b", "s")
    val _: df.IntColumn = df.column[Int]("i")
    val _: df.LongColumn = df.column[Long]("l")
    val _: df.DoubleColumn = df.column[Double]("d")
    val _: df.FloatColumn = df.column[Float]("f")
    val _: df.BooleanColumn = df.column[Boolean]("b")
    val _: df.ObjectColumn[String] = df.column[String]("s")
    succeed

  test("Accessing a column with the wrong type fails"):
    val df = DataFrame.fromSeq(Seq((1, "a"))).renameColumns("i", "s")
    assertThrows[IncorrectTypeException](df.column[Long]("i"))
    assertThrows[IncorrectTypeException](df.column[Int]("s"))
    assertThrows[NoSuchElementException](df.column[Int]("unknown"))
