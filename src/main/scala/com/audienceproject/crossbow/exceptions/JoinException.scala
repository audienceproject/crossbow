package com.audienceproject.crossbow.exceptions

import com.audienceproject.crossbow.expr.Expr

class JoinException(joinExpr: Expr)
  extends RuntimeException(s"$joinExpr does not evaluate to the same type on both sides of the join.")
