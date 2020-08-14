package com.audienceproject.crossbow.expr

private[crossbow] object Types {

  def typecheck(internalType: Type, runtimeType: ru.Type): Boolean = internalType match {
    case IntType if ru.definitions.IntTpe weak_<:< runtimeType => true
    case LongType if ru.definitions.LongTpe weak_<:< runtimeType => true
    case DoubleType if ru.definitions.DoubleTpe weak_<:< runtimeType => true
    case BooleanType if ru.definitions.BooleanTpe weak_<:< runtimeType => true
    case AnyType(anyType) if anyType <:< runtimeType => true
    case ProductType(elementTypes@_*) if runtimeType <:< ru.typeOf[Product] =>
      val innerTypes = runtimeType.typeArgs
      if (elementTypes.length == innerTypes.length)
        elementTypes.zip(innerTypes).forall({ case (e, r) => typecheck(e, r) })
      else false
    case ListType(elementType) if runtimeType <:< ru.typeOf[Seq[_]] =>
      typecheck(elementType, runtimeType)
    case _ => false
  }

  def toInternalType(runtimeType: ru.Type): Type = runtimeType match {
    case t if t =:= ru.definitions.IntTpe => IntType
    case t if t =:= ru.definitions.LongTpe => LongType
    case t if t =:= ru.definitions.DoubleTpe => DoubleType
    case t if t =:= ru.definitions.BooleanTpe => BooleanType
    case t if t <:< ru.typeOf[Product] => ProductType(t.typeArgs.map(toInternalType): _*)
    case t if t <:< ru.typeOf[Seq[_]] => ListType(toInternalType(t.typeArgs.head))
    case _ => AnyType(runtimeType)
  }

}
