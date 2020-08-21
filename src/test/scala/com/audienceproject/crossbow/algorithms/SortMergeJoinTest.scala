package com.audienceproject.crossbow.algorithms

import com.audienceproject.crossbow.DataFrame
import com.audienceproject.crossbow.Implicits._
import org.scalatest.funsuite.AnyFunSuite

class SortMergeJoinTest extends AnyFunSuite {

  test("Test join") {
    val left = DataFrame.fromSeq(Seq(("a", 1), ("b", 2), ("c", 3), ("d", 4)))
    val right = DataFrame.fromSeq(Seq(("a", true), ("b", false), ("c", true)))
    val joined = left.join(right, $"_0")
    joined.foreach(println)
  }

}
