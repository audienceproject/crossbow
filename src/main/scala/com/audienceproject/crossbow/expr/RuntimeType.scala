package com.audienceproject.crossbow.expr

enum RuntimeType:
  case Int, Long, Double, Boolean, String, Float
  case Product(elementTypes: RuntimeType*)
  case List(elementType: RuntimeType)
  case Generic(typeName: String)

  def compatible(actual: RuntimeType): Boolean = (this, actual) match
    case (Product(xs*), Product(ys*)) if xs.length == ys.length => xs.zip(ys).forall(_ compatible _)
    case (List(x), List(y)) => x compatible y
    case (Generic("scala.Any"), _) => true // TODO: Make an explicit RuntimeType.Any - the macro isn't obvious though
    case (Generic(_), Generic(_)) => true
    case (x, y) => x == y

  override def toString: String = this match
    case Int => "int"
    case Long => "long"
    case Double => "double"
    case Boolean => "boolean"
    case String => "string"
    case Float => "float"
    case Product(elementTypes*) => s"(${elementTypes.mkString(",")})"
    case List(elementType) => s"List($elementType)"
    case Generic(typeName) => typeName
