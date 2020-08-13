package com.audienceproject.crossbow.expr

import com.audienceproject.crossbow.exceptions.NoOrderingException

class Order private(private val ord: Ordering[_], private val typeOf: ru.Type)

object Order {

  def on[T](implicit ord: Ordering[T], t: ru.TypeTag[T]): Order = new Order(ord, ru.typeOf[T])

  private[crossbow] def getOrdering(typeOf: ru.Type, givens: Seq[Order]): Ordering[Any] = {
    val ord = typeOf match {
      case IntType => Ordering.Int
      case LongType => Ordering.Long
      case DoubleType => Ordering.Double
      case BooleanType => Ordering.Boolean
      case t if t =:= ru.typeOf[String] => Ordering.String
      case p if p <:< ru.typeOf[Product] =>
        val tupleTypes = p.typeArgs.map(getOrdering(_, givens))
        tupleTypes match {
          case List(o1, o2) => Ordering.Tuple2(o1, o2)
          case List(o1, o2, o3) => Ordering.Tuple3(o1, o2, o3)
          case List(o1, o2, o3, o4) => Ordering.Tuple4(o1, o2, o3, o4)
          case List(o1, o2, o3, o4, o5) => Ordering.Tuple5(o1, o2, o3, o4, o5)
          case List(o1, o2, o3, o4, o5, o6) => Ordering.Tuple6(o1, o2, o3, o4, o5, o6)
        }
      case other =>
        val givenMatch = givens.find(_.typeOf =:= other)
        givenMatch.getOrElse(throw new NoOrderingException(typeOf)).ord
    }
    ord.asInstanceOf[Ordering[Any]]
  }

}
