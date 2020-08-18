package com.audienceproject.crossbow.expr

import com.audienceproject.crossbow.DataFrame
import com.audienceproject.crossbow.expr.Aggregator.Reducer

abstract class Aggregator(expr: Expr) {

  private[crossbow] def compile(context: DataFrame): Reducer[Any, Any] = {
    val op = expr.compile(context)
    typeSpec(op).asInstanceOf[Reducer[Any, Any]]
  }

  protected def typeSpec(op: Specialized[_]): Reducer[_, _]

}

private[crossbow] object Aggregator {

  case class Reducer[T, U](specialized: Specialized[T], f: (T, U) => U, seed: U, typeOf: Type) {
    def apply(i: Int, agg: U): U = f(specialized(i), agg)
  }

}
