package com.audienceproject.crossbow.expr

enum RuntimeType:
  case Int, Long, Double, Boolean, String
  case Any(typeName: String)
  case Product(elementTypes: RuntimeType*)
  case List(elementType: RuntimeType)

  override def toString: String = this match
    case Int => "int"
    case Long => "long"
    case Double => "double"
    case Boolean => "boolean"
    case String => "string"
    case Any(typeName) => typeName
    case Product(elementTypes*) => s"(${elementTypes.mkString(",")})"
    case List(elementType) => s"List($elementType)"
