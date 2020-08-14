package com.audienceproject.crossbow.exceptions

import com.audienceproject.crossbow.expr.Type

class NoOrderingException(arg: Type)
  extends RuntimeException(s"Could not determine a natural or given ordering on type $arg")
