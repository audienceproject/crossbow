package com.audienceproject.crossbow

sealed trait JoinType

object JoinType {

  case object Inner extends JoinType

  case object FullOuter extends JoinType

  case object LeftOuter extends JoinType

  case object RightOuter extends JoinType

}
