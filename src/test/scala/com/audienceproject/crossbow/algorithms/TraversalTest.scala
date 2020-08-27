package com.audienceproject.crossbow.algorithms

import com.audienceproject.crossbow.Implicits._
import com.audienceproject.crossbow.expr.{Aggregator, Expr}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable

class TraversalTest extends AnyFunSuite {

  private val aggExpr = sum($"x") + count($"y") / 2 * collect($"z")

  test("Transform expression tree") {
    val transformedExpr = Traversal.transform(aggExpr, {
      case _: Aggregator => Expr.Cell("42")
    })
    val expected = $"42" + $"42" / 2 * $"42"
    assert(transformedExpr == expected)
  }

  test("Transform with stateful side-effects") {
    val list = mutable.ListBuffer.empty[Expr]
    val transformedExpr = Traversal.transform(aggExpr, {
      case _: Aggregator =>
        val newCol = Expr.Cell(s"_${list.size}")
        list += newCol
        newCol
    })
    val expected = $"_0" + $"_1" / 2 * $"_2"
    assert(transformedExpr == expected)
    assert(list == Seq($"_0", $"_1", $"_2"))
  }

}
