package com.audienceproject.crossbow

import com.audienceproject.crossbow.exceptions.AggregationException
import com.audienceproject.crossbow.expr.*

enum JoinType:
  case Inner, FullOuter, LeftOuter, RightOuter

export expr.Order

// Column expression.
extension (sc: StringContext)
  def $(args: Any*): DataFrame ?=> Expr = Expr.Cell(sc.s(args *))

extension [T: TypeTag](seq: Seq[T])
  def toDataFrame(names: String*): DataFrame =
    if (names.isEmpty) DataFrame.fromSeq[T](seq)
    else DataFrame.fromSeq[T](seq).renameColumns(names *)

// Literal value.
def lit[T: TypeTag](value: T): Expr = Expr.Literal(value)

given Conversion[Int, Expr] = lit[Int](_)
given Conversion[Long, Expr] = lit[Long](_)
given Conversion[Double, Expr] = lit[Double](_)
given Conversion[Boolean, Expr] = lit[Boolean](_)
given Conversion[String, Expr] = lit[String](_)

// Index function.
def index(): DataFrame ?=> Expr = Expr.Index()

// Lambda function.
def lambda[T: TypeTag, R: TypeTag](f: T => R): Expr => Expr =
  (expr: Expr) => Expr.Unary(expr, f)

// Product type (tuples).
private def tupleToExprs[T <: Tuple](tup: T): List[Expr] = tup match
  // TODO: Is there some way to ensure a Tuple of Expr instead of casting?
  case head *: tail => head.asInstanceOf[Expr] :: tupleToExprs(tail)
  case EmptyTuple => Nil

given fromTuple[T <: Tuple]: Conversion[T, Expr] = (tup: T) => Expr.Product(tupleToExprs(tup))

// Sequence of values.
def seq(exprs: Expr*): Expr = Expr.List(exprs)

// Aggregators.
def sum(expr: Expr): Expr = expr.typeOf match
  case RuntimeType.Int => Expr.Aggregate[Int, Int](expr)(_ + _, 0)
  case RuntimeType.Long => Expr.Aggregate[Long, Long](expr)(_ + _, 0L)
  case RuntimeType.Double => Expr.Aggregate[Double, Double](expr)(_ + _, 0d)
  case _ => throw new AggregationException(expr)

def count(): Expr =
  Expr.Aggregate[Any, Int](lit(1))((_, x) => x + 1, 0, RuntimeType.Int)

def collect(expr: Expr): Expr =
  Expr.Aggregate[Any, Seq[Any]](expr)((e, seq) => seq :+ e, Vector.empty, RuntimeType.List(expr.typeOf))

def one(expr: Expr): Expr =
  Expr.Aggregate[Any, Any](expr)((elem, _) => elem, null)

// Custom aggregator.
def reducer[T: TypeTag, U: TypeTag](seed: U)(f: (T, U) => U): Expr => Expr =
  Expr.Aggregate[T, U](_)(f, seed, summon[TypeTag[U]].runtimeType)
