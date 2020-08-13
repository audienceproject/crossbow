package com.audienceproject.crossbow.exceptions

import com.audienceproject.crossbow.expr.ru

class NoOrderingException(arg: ru.Type)
  extends RuntimeException(s"Could not determine a natural or given ordering on type $arg")
