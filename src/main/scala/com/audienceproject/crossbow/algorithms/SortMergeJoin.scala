package com.audienceproject.crossbow.algorithms

import com.audienceproject.crossbow.exceptions.JoinException
import com.audienceproject.crossbow.expr.{Expr, Order}
import com.audienceproject.crossbow.{DataFrame, JoinType}

import scala.annotation.tailrec
import scala.collection.mutable

private[crossbow] object SortMergeJoin {

  private val joinColName = "_joinExpr"

  def apply(left: DataFrame, right: DataFrame, joinExpr: Expr, joinType: JoinType): DataFrame = {
    val internalType = joinExpr.compile(left).typeOf
    if (internalType != joinExpr.compile(right).typeOf) throw new JoinException(joinExpr)

    val ordering = Order.getOrdering(internalType)

    val leftSorted = left.addColumn(joinExpr as joinColName).sortBy(Expr.Column(joinColName))
    val rightSorted = right.addColumn(joinExpr as joinColName).sortBy(Expr.Column(joinColName))

    val leftKey = leftSorted(joinColName).as[Any]
    val rightKey = rightSorted(joinColName).as[Any]

    val (leftResult, rightResult) = resultBuffers(left, right, joinType)

    def advance(start: Int, keyColumn: DataFrame#TypedView[Any]): Seq[Int] = {
      if (start >= keyColumn.size) Seq.empty
      else {
        val key = keyColumn(start)

        @tailrec
        def indexOfNextKey(i: Int): Int = {
          if (i < keyColumn.size) {
            val nextKey = keyColumn(i)
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

    if (leftSet.nonEmpty && isLeftJoin) addCartesianProduct(leftSet.head until leftSet.size, Seq(-1))
    if (rightSet.nonEmpty && isRightJoin) addCartesianProduct(Seq(-1), rightSet.head until rightKey.size)

    val leftFinal = leftSorted.slice(leftResult.toIndexedSeq).removeColumns(joinColName)
    val rightFinal = rightSorted.slice(rightResult.toIndexedSeq).removeColumns(joinColName)
    leftFinal.merge(rightFinal)
  }

  private def resultBuffers(left: DataFrame, right: DataFrame,
                            joinType: JoinType): (mutable.ArrayBuffer[Int], mutable.ArrayBuffer[Int]) = {
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
