package com.audienceproject.crossbow.expr

abstract class Expr extends BaseOps with ArithmeticOps with BooleanOps with ComparisonOps {
  private[crossbow] def compile(): Specialized[_]
}
