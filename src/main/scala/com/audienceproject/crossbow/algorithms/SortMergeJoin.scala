package com.audienceproject.crossbow.algorithms

import com.audienceproject.crossbow.expr.Expr
import com.audienceproject.crossbow.{DataFrame, JoinType}

import scala.annotation.tailrec
import scala.collection.mutable

private[crossbow] object SortMergeJoin {

  // NOTE: 'left' and 'right' are assumed to be sorted on 'joinExpr' in advance.
  def apply(left: DataFrame, right: DataFrame, joinExpr: DataFrame ?=> Expr, joinType: JoinType, ordering: Ordering[Any]): DataFrame = {

    val leftKey = left.select(joinExpr)
    val rightKey = right.select(joinExpr)

    val (leftResult, rightResult) = resultBuffers(left, right, joinType)

    def advance(start: Int, df: DataFrame): Seq[Int] = {
      if (start >= df.rowCount) Seq.empty
      else {
        val key = df(start)

        @tailrec
        def indexOfNextKey(i: Int): Int = {
          if (i < df.rowCount) {
            val nextKey = df(i)
            if (ordering.equiv(key, nextKey)) indexOfNextKey(i + 1)
            else i
          } else i
        }

        start until indexOfNextKey(start + 1)
      }
    }

    def addCartesianProduct(leftSet: Seq[Int], rightSet: Seq[Int]): Unit =
      for (x <- leftSet; y <- rightSet) {
        leftResult += x
        rightResult += y
      }

    val isLeftJoin = joinType == JoinType.LeftOuter || joinType == JoinType.FullOuter
    val isRightJoin = joinType == JoinType.RightOuter || joinType == JoinType.FullOuter

    var leftSet = advance(0, leftKey)
    var rightSet = advance(0, rightKey)
    while (leftSet.nonEmpty && rightSet.nonEmpty) {
      val cmp = ordering.compare(leftKey(leftSet.head), rightKey(rightSet.head))
      if (cmp == 0) {
        addCartesianProduct(leftSet, rightSet)
        leftSet = advance(leftSet.last + 1, leftKey)
        rightSet = advance(rightSet.last + 1, rightKey)
      } else if (cmp < 0) {
        if (isLeftJoin) addCartesianProduct(leftSet, Seq(-1))
        leftSet = advance(leftSet.last + 1, leftKey)
      } else {
        if (isRightJoin) addCartesianProduct(Seq(-1), rightSet)
        rightSet = advance(rightSet.last + 1, rightKey)
      }
    }

    if (leftSet.nonEmpty && isLeftJoin) addCartesianProduct(leftSet.head until leftKey.rowCount, Seq(-1))
    if (rightSet.nonEmpty && isRightJoin) addCartesianProduct(Seq(-1), rightSet.head until rightKey.rowCount)

    val leftFinal = left.slice(leftResult.toIndexedSeq)
    val rightFinal = right.slice(rightResult.toIndexedSeq)
    leftFinal.merge(rightFinal)
  }

  private def resultBuffers(left: DataFrame, right: DataFrame, joinType: JoinType): (mutable.ArrayBuffer[Int], mutable.ArrayBuffer[Int]) = {
    val defaultBufferSize = joinType match {
      case JoinType.Inner => math.min(left.rowCount, right.rowCount)
      case JoinType.FullOuter => math.max(left.rowCount, right.rowCount)
      case JoinType.LeftOuter => left.rowCount
      case JoinType.RightOuter => right.rowCount
    }

    def initBuffer() = new mutable.ArrayBuffer[Int](defaultBufferSize)

    (initBuffer(), initBuffer())
  }

}
