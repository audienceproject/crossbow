package com.audienceproject.crossbow.algorithms

import com.audienceproject.crossbow.Implicits._
import com.audienceproject.crossbow.{DataFrame, JoinType}
import org.scalatest.funsuite.AnyFunSuite

class SortMergeJoinTest extends AnyFunSuite {

  private val left = DataFrame.fromSeq(Seq(("b", 2), ("a", 1), ("d", 4), ("c", 3), ("e", 5), ("f", 6)))
  private val right = DataFrame.fromSeq(Seq(("a", true), ("b", false), ("f", true), ("f", false), ("x", false)))

  test("Inner join") {
    val joined = left.join(right, $"_0").select($"_0", $"_1", $"#_1").as[(String, Int, Boolean)].toSeq
    val expected = Seq(("a", 1, true), ("b", 2, false), ("f", 6, true), ("f", 6, false))
    assert(joined == expected)
  }

  test("Left outer join") {
    val joined = left.join(right, $"_0", JoinType.LeftOuter)
      .select($"_0", $"#_0", $"_1", $"#_1").as[(String, String, Int, Boolean)].toSeq
    val expected = Seq(("a", "a", 1, true), ("b", "b", 2, false), ("c", null, 3, false), ("d", null, 4, false),
      ("e", null, 5, false), ("f", "f", 6, true), ("f", "f", 6, false))
    assert(joined == expected)
  }

  test("Right outer join") {
    val joined = left.join(right, $"_0", JoinType.RightOuter)
      .select($"_0", $"#_0", $"_1", $"#_1").as[(String, String, Int, Boolean)].toSeq
    val expected = Seq(("a", "a", 1, true), ("b", "b", 2, false), ("f", "f", 6, true), ("f", "f", 6, false),
      (null, "x", 0, false))
    assert(joined == expected)
  }

  test("Full outer join") {
    val joined = left.join(right, $"_0", JoinType.FullOuter)
      .select($"_0", $"#_0", $"_1", $"#_1").as[(String, String, Int, Boolean)].toSeq
    val expected = Seq(("a", "a", 1, true), ("b", "b", 2, false), ("c", null, 3, false), ("d", null, 4, false),
      ("e", null, 5, false), ("f", "f", 6, true), ("f", "f", 6, false), (null, "x", 0, false))
    assert(joined == expected)
  }

  test("Join on pre-sorted DataFrame") {
    val other = DataFrame.fromSeq(Seq("b", "f", "e"))
    val joined = left.sortBy($"_0").join(other, $"_0").select($"_0", $"_1").as[(String, Int)].toSeq
    val expected = Seq(("b", 2), ("e", 5), ("f", 6))
    assert(joined == expected)
  }

}
