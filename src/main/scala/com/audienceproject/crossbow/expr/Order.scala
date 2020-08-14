package com.audienceproject.crossbow.expr

import com.audienceproject.crossbow.exceptions.NoOrderingException

class Order private(private val ord: Ordering[_], private val typeOf: ru.Type)

object Order {

  def on[T: Ordering : ru.TypeTag]: Order = new Order(implicitly[Ordering[T]], ru.typeOf[T])

  private[crossbow] def getOrdering(internalType: Type, givens: Seq[Order]): Ordering[Any] = {
    val ord = internalType match {
      case IntType => Ordering.Int
      case LongType => Ordering.Long
      case DoubleType => Ordering.Double
      case BooleanType => Ordering.Boolean
      case ProductType(elementTypes@_*) =>
        val tupleTypes = elementTypes.map(getOrdering(_, givens))
        tupleTypes match {
          case Seq(o1, o2) => Ordering.Tuple2(o1, o2)
          case Seq(o1, o2, o3) => Ordering.Tuple3(o1, o2, o3)
          case Seq(o1, o2, o3, o4) => Ordering.Tuple4(o1, o2, o3, o4)
          case Seq(o1, o2, o3, o4, o5) => Ordering.Tuple5(o1, o2, o3, o4, o5)
          case Seq(o1, o2, o3, o4, o5, o6) => Ordering.Tuple6(o1, o2, o3, o4, o5, o6)
        }
      case listType: ListType => throw new NoOrderingException(listType)
      case AnyType(runtimeType) if runtimeType =:= ru.typeOf[String] => Ordering.String
      case AnyType(runtimeType) =>
        val givenMatch = givens.find(_.typeOf =:= runtimeType)
        givenMatch.getOrElse(throw new NoOrderingException(internalType)).ord
    }
    ord.asInstanceOf[Ordering[Any]]
  }

}
