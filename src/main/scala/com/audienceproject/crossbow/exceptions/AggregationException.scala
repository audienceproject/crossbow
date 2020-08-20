package com.audienceproject.crossbow.exceptions

import com.audienceproject.crossbow.expr.Expr

class AggregationException(arg: Expr)
  extends RuntimeException(s"Invalid use of aggregation expression at $arg")
