package com.audienceproject.crossbow.expr

import com.audienceproject.crossbow.exceptions.{IncorrectTypeException, NoOrderingException}

enum Order:
  case Implicit
  case Explicit[T](private val ordering: Ordering[T], private val onType: RuntimeType)

  private[crossbow] def getOrdering(runtimeType: RuntimeType): Ordering[?] = this match
    case Order.Implicit => runtimeType match
      case RuntimeType.Int => Ordering.Int
      case RuntimeType.Long => Ordering.Long
      case RuntimeType.Double => Ordering.Double.TotalOrdering
      case RuntimeType.Boolean => Ordering.Boolean
      case RuntimeType.String => Ordering.String
      case RuntimeType.Product(elementTypes*) =>
        elementTypes.map(getOrdering) match
          case Seq(o1, o2) => Ordering.Tuple2(o1, o2)
          case Seq(o1, o2, o3) => Ordering.Tuple3(o1, o2, o3)
          case Seq(o1, o2, o3, o4) => Ordering.Tuple4(o1, o2, o3, o4)
          case Seq(o1, o2, o3, o4, o5) => Ordering.Tuple5(o1, o2, o3, o4, o5)
          case Seq(o1, o2, o3, o4, o5, o6) => Ordering.Tuple6(o1, o2, o3, o4, o5, o6)
          case _ => throw NoOrderingException(runtimeType)
      case RuntimeType.List(elementType) => Ordering.Implicits.seqOrdering(getOrdering(elementType))
      case _ => throw NoOrderingException(runtimeType)
    case Order.Explicit(ordering, onType) =>
      if onType.compatible(runtimeType) then ordering
      else throw IncorrectTypeException(onType, runtimeType)

object Order:
  def by[T: TypeTag](ord: Ordering[T]): Order = Explicit(ord, summon[TypeTag[T]].runtimeType)
