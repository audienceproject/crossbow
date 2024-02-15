package com.audienceproject.crossbow.exceptions

import com.audienceproject.crossbow.expr.RuntimeType

class NoOrderingException(arg: RuntimeType)
  extends RuntimeException(s"Could not determine a natural or given ordering on type $arg")
