package com.audienceproject.crossbow.expr

import scala.quoted.*

trait TypeTag[T]:
  val runtimeType: RuntimeType

object TypeTag:

  inline def getRuntimeType[T]: RuntimeType = ${ getRuntimeTypeImpl[T] }

  private def getRuntimeTypeImpl[T: Type](using Quotes): Expr[RuntimeType] =
    Type.of[T] match
      case '[Int] => '{ RuntimeType.Int }
      case '[Long] => '{ RuntimeType.Long }
      case '[Double] => '{ RuntimeType.Double }
      case '[Boolean] => '{ RuntimeType.Boolean }
      case '[java.lang.String] => '{ RuntimeType.String }
      case '[head *: tail] =>
        Expr.ofList(tupleToList[head *: tail]) match
          case '{ $productTypes } => '{ RuntimeType.Product(${ productTypes } *) }
      case _ => '{ RuntimeType.Any(${ Expr(Type.show[T]) }) }

  private def tupleToList[T: Type](using Quotes): List[Expr[RuntimeType]] =
    Type.of[T] match
      case '[head *: tail] => getRuntimeTypeImpl[head] :: tupleToList[tail]
      case _ => Nil

inline given typeTag[T]: TypeTag[T] = new TypeTag[T]:
  override val runtimeType: RuntimeType = TypeTag.getRuntimeType[T]
