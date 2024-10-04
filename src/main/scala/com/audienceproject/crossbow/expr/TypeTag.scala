package com.audienceproject.crossbow.expr

import scala.quoted.*

trait TypeTag[T]:
  val runtimeType: RuntimeType

object TypeTag:

  inline given of[T]: TypeTag[T] = new TypeTag[T]:
    override val runtimeType: RuntimeType = getRuntimeType[T]

  private inline def getRuntimeType[T]: RuntimeType = ${ getRuntimeTypeImpl[T] }

  private def getRuntimeTypeImpl[T: Type](using Quotes): Expr[RuntimeType] =
    Type.of[T] match
      case '[Int] => '{ RuntimeType.Int }
      case '[Long] => '{ RuntimeType.Long }
      case '[Double] => '{ RuntimeType.Double }
      case '[Float] => '{ RuntimeType.Float }
      case '[Boolean] => '{ RuntimeType.Boolean }
      case '[String] => '{ RuntimeType.String }
      case '[head *: tail] => '{ RuntimeType.Product(${ Expr.ofList(tupleToList[head *: tail]) } *) }
      case '[Seq[t]] => '{ RuntimeType.List(${ getRuntimeTypeImpl[t] }) }
      case _ => '{ RuntimeType.Generic(${ Expr(Type.show[T]) }) }

  private def tupleToList[T: Type](using Quotes): List[Expr[RuntimeType]] =
    Type.of[T] match
      case '[head *: tail] => getRuntimeTypeImpl[head] :: tupleToList[tail]
      case _ => Nil
