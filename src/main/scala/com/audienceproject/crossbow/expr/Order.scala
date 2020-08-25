package com.audienceproject.crossbow.expr

import com.audienceproject.crossbow.exceptions.NoOrderingException

class Order private(private val ord: Ordering[_], private val internalType: Type)

object Order {

  def by[T: ru.TypeTag](ord: Ordering[T]): Order = new Order(ord, Types.toInternalType(ru.typeOf[T]))

  private[crossbow] def getOrdering(internalType: Type, givens: Seq[Order] = Seq.empty): Ordering[Any] = {
    givens.find(_.internalType == internalType) match {
      case Some(explicitOrdering) => explicitOrdering.ord.asInstanceOf[Ordering[Any]]
      case None =>
        val implicitOrdering = internalType match {
          case IntType => Ordering.Int
          case LongType => Ordering.Long
          case DoubleType => DoubleOrdering
          case BooleanType => Ordering.Boolean
          case ProductType(elementTypes@_*) =>
            val tupleTypes = elementTypes.map(getOrdering(_, givens))
            tupleTypes match {
              case Seq(o1, o2) => Ordering.Tuple2(o1, o2)
              case Seq(o1, o2, o3) => Ordering.Tuple3(o1, o2, o3)
              case Seq(o1, o2, o3, o4) => Ordering.Tuple4(o1, o2, o3, o4)
              case Seq(o1, o2, o3, o4, o5) => Ordering.Tuple5(o1, o2, o3, o4, o5)
              case Seq(o1, o2, o3, o4, o5, o6) => Ordering.Tuple6(o1, o2, o3, o4, o5, o6)
              case _ => throw new NoOrderingException(internalType)
            }
          case _: ListType => throw new NoOrderingException(internalType)
          case AnyType(runtimeType) if runtimeType =:= ru.typeOf[String] => Ordering.String
          case _ => throw new NoOrderingException(internalType)
        }
        implicitOrdering.asInstanceOf[Ordering[Any]]
    }
  }

  private object DoubleOrdering extends Ordering[Double] {
    override def compare(x: Double, y: Double): Int = java.lang.Double.compare(x, y)
  }

}
