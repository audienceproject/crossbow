package com.audienceproject.crossbow.expr

import com.audienceproject.crossbow.exceptions.InvalidExpressionException

import scala.annotation.targetName

trait ArithmeticOps:

  x: Expr =>

  @targetName("plus")
  def +(y: Expr): Expr = (x.typeOf, y.typeOf) match
    // Long
    case (RuntimeType.Long, RuntimeType.Double) => Expr.Binary[Long, Double, Double](x, y, _ + _)
    case (RuntimeType.Long, RuntimeType.Float) => Expr.Binary[Long, Float, Float](x, y, _ + _)
    case (RuntimeType.Long, RuntimeType.Int) => Expr.Binary[Long, Int, Long](x, y, _ + _)
    case (RuntimeType.Long, RuntimeType.Long) => Expr.Binary[Long, Long, Long](x, y, _ + _)
    // Int
    case (RuntimeType.Int, RuntimeType.Double) => Expr.Binary[Int, Double, Double](x, y, _ + _)
    case (RuntimeType.Int, RuntimeType.Float) => Expr.Binary[Int, Float, Float](x, y, _ + _)
    case (RuntimeType.Int, RuntimeType.Int) => Expr.Binary[Int, Int, Int](x, y, _ + _)
    case (RuntimeType.Int, RuntimeType.Long) => Expr.Binary[Int, Long, Long](x, y, _ + _)
    // Double
    case (RuntimeType.Double, RuntimeType.Double) => Expr.Binary[Double, Double, Double](x, y, _ + _)
    case (RuntimeType.Double, RuntimeType.Float) => Expr.Binary[Double, Float, Double](x, y, _ + _)
    case (RuntimeType.Double, RuntimeType.Int) => Expr.Binary[Double, Int, Double](x, y, _ + _)
    case (RuntimeType.Double, RuntimeType.Long) => Expr.Binary[Double, Long, Double](x, y, _ + _)
    // Float
    case (RuntimeType.Float, RuntimeType.Double) => Expr.Binary[Float, Double, Double](x, y, _ + _)
    case (RuntimeType.Float, RuntimeType.Float) => Expr.Binary[Float, Float, Float](x, y, _ + _)
    case (RuntimeType.Float, RuntimeType.Int) => Expr.Binary[Float, Int, Float](x, y, _ + _)
    case (RuntimeType.Float, RuntimeType.Long) => Expr.Binary[Float, Long, Float](x, y, _ + _)
    case _ => throw new InvalidExpressionException("plus", x, y)

  @targetName("minus")
  def -(y: Expr): Expr = (x.typeOf, y.typeOf) match
    // Long
    case (RuntimeType.Long, RuntimeType.Double) => Expr.Binary[Long, Double, Double](x, y, _ - _)
    case (RuntimeType.Long, RuntimeType.Float) => Expr.Binary[Long, Float, Float](x, y, _ - _)
    case (RuntimeType.Long, RuntimeType.Int) => Expr.Binary[Long, Int, Long](x, y, _ - _)
    case (RuntimeType.Long, RuntimeType.Long) => Expr.Binary[Long, Long, Long](x, y, _ - _)
    // Int
    case (RuntimeType.Int, RuntimeType.Double) => Expr.Binary[Int, Double, Double](x, y, _ - _)
    case (RuntimeType.Int, RuntimeType.Float) => Expr.Binary[Int, Float, Float](x, y, _ - _)
    case (RuntimeType.Int, RuntimeType.Int) => Expr.Binary[Int, Int, Int](x, y, _ - _)
    case (RuntimeType.Int, RuntimeType.Long) => Expr.Binary[Int, Long, Long](x, y, _ - _)
    // Double
    case (RuntimeType.Double, RuntimeType.Double) => Expr.Binary[Double, Double, Double](x, y, _ - _)
    case (RuntimeType.Double, RuntimeType.Float) => Expr.Binary[Double, Float, Double](x, y, _ - _)
    case (RuntimeType.Double, RuntimeType.Int) => Expr.Binary[Double, Int, Double](x, y, _ - _)
    case (RuntimeType.Double, RuntimeType.Long) => Expr.Binary[Double, Long, Double](x, y, _ - _)
    // Float
    case (RuntimeType.Float, RuntimeType.Double) => Expr.Binary[Float, Double, Double](x, y, _ - _)
    case (RuntimeType.Float, RuntimeType.Float) => Expr.Binary[Float, Float, Float](x, y, _ - _)
    case (RuntimeType.Float, RuntimeType.Int) => Expr.Binary[Float, Int, Float](x, y, _ - _)
    case (RuntimeType.Float, RuntimeType.Long) => Expr.Binary[Float, Long, Float](x, y, _ - _)
    case _ => throw new InvalidExpressionException("minus", x, y)

  @targetName("times")
  def *(y: Expr): Expr = (x.typeOf, y.typeOf) match
    // Long
    case (RuntimeType.Long, RuntimeType.Double) => Expr.Binary[Long, Double, Double](x, y, _ * _)
    case (RuntimeType.Long, RuntimeType.Float) => Expr.Binary[Long, Float, Float](x, y, _ * _)
    case (RuntimeType.Long, RuntimeType.Int) => Expr.Binary[Long, Int, Long](x, y, _ * _)
    case (RuntimeType.Long, RuntimeType.Long) => Expr.Binary[Long, Long, Long](x, y, _ * _)
    // Int
    case (RuntimeType.Int, RuntimeType.Double) => Expr.Binary[Int, Double, Double](x, y, _ * _)
    case (RuntimeType.Int, RuntimeType.Float) => Expr.Binary[Int, Float, Float](x, y, _ * _)
    case (RuntimeType.Int, RuntimeType.Int) => Expr.Binary[Int, Int, Int](x, y, _ * _)
    case (RuntimeType.Int, RuntimeType.Long) => Expr.Binary[Int, Long, Long](x, y, _ * _)
    // Double
    case (RuntimeType.Double, RuntimeType.Double) => Expr.Binary[Double, Double, Double](x, y, _ * _)
    case (RuntimeType.Double, RuntimeType.Float) => Expr.Binary[Double, Float, Double](x, y, _ * _)
    case (RuntimeType.Double, RuntimeType.Int) => Expr.Binary[Double, Int, Double](x, y, _ * _)
    case (RuntimeType.Double, RuntimeType.Long) => Expr.Binary[Double, Long, Double](x, y, _ * _)
    // Float
    case (RuntimeType.Float, RuntimeType.Double) => Expr.Binary[Float, Double, Double](x, y, _ * _)
    case (RuntimeType.Float, RuntimeType.Float) => Expr.Binary[Float, Float, Float](x, y, _ * _)
    case (RuntimeType.Float, RuntimeType.Int) => Expr.Binary[Float, Int, Float](x, y, _ * _)
    case (RuntimeType.Float, RuntimeType.Long) => Expr.Binary[Float, Long, Float](x, y, _ * _)
    case _ => throw new InvalidExpressionException("times", x, y)

  @targetName("div")
  def /(y: Expr): Expr = (x.typeOf, y.typeOf) match
    // Long
    case (RuntimeType.Long, RuntimeType.Double) => Expr.Binary[Long, Double, Double](x, y, _ / _)
    case (RuntimeType.Long, RuntimeType.Float) => Expr.Binary[Long, Float, Float](x, y, _ / _)
    case (RuntimeType.Long, RuntimeType.Int) => Expr.Binary[Long, Int, Long](x, y, _ / _)
    case (RuntimeType.Long, RuntimeType.Long) => Expr.Binary[Long, Long, Long](x, y, _ / _)
    // Int
    case (RuntimeType.Int, RuntimeType.Double) => Expr.Binary[Int, Double, Double](x, y, _ / _)
    case (RuntimeType.Int, RuntimeType.Float) => Expr.Binary[Int, Float, Float](x, y, _ / _)
    case (RuntimeType.Int, RuntimeType.Int) => Expr.Binary[Int, Int, Int](x, y, _ / _)
    case (RuntimeType.Int, RuntimeType.Long) => Expr.Binary[Int, Long, Long](x, y, _ / _)
    // Double
    case (RuntimeType.Double, RuntimeType.Double) => Expr.Binary[Double, Double, Double](x, y, _ / _)
    case (RuntimeType.Double, RuntimeType.Float) => Expr.Binary[Double, Float, Double](x, y, _ / _)
    case (RuntimeType.Double, RuntimeType.Int) => Expr.Binary[Double, Int, Double](x, y, _ / _)
    case (RuntimeType.Double, RuntimeType.Long) => Expr.Binary[Double, Long, Double](x, y, _ / _)
    // Float
    case (RuntimeType.Float, RuntimeType.Double) => Expr.Binary[Float, Double, Double](x, y, _ / _)
    case (RuntimeType.Float, RuntimeType.Float) => Expr.Binary[Float, Float, Float](x, y, _ / _)
    case (RuntimeType.Float, RuntimeType.Int) => Expr.Binary[Float, Int, Float](x, y, _ / _)
    case (RuntimeType.Float, RuntimeType.Long) => Expr.Binary[Float, Long, Float](x, y, _ / _)
    case _ => throw new InvalidExpressionException("div", x, y)

  @targetName("mod")
  def %(y: Expr): Expr = (x.typeOf, y.typeOf) match
    // Long
    case (RuntimeType.Long, RuntimeType.Double) => Expr.Binary[Long, Double, Double](x, y, _ % _)
    case (RuntimeType.Long, RuntimeType.Float) => Expr.Binary[Long, Float, Float](x, y, _ % _)
    case (RuntimeType.Long, RuntimeType.Int) => Expr.Binary[Long, Int, Long](x, y, _ % _)
    case (RuntimeType.Long, RuntimeType.Long) => Expr.Binary[Long, Long, Long](x, y, _ % _)
    // Int
    case (RuntimeType.Int, RuntimeType.Double) => Expr.Binary[Int, Double, Double](x, y, _ % _)
    case (RuntimeType.Int, RuntimeType.Float) => Expr.Binary[Int, Float, Float](x, y, _ % _)
    case (RuntimeType.Int, RuntimeType.Int) => Expr.Binary[Int, Int, Int](x, y, _ % _)
    case (RuntimeType.Int, RuntimeType.Long) => Expr.Binary[Int, Long, Long](x, y, _ % _)
    // Double
    case (RuntimeType.Double, RuntimeType.Double) => Expr.Binary[Double, Double, Double](x, y, _ % _)
    case (RuntimeType.Double, RuntimeType.Float) => Expr.Binary[Double, Float, Double](x, y, _ % _)
    case (RuntimeType.Double, RuntimeType.Int) => Expr.Binary[Double, Int, Double](x, y, _ % _)
    case (RuntimeType.Double, RuntimeType.Long) => Expr.Binary[Double, Long, Double](x, y, _ % _)
    // Float
    case (RuntimeType.Float, RuntimeType.Double) => Expr.Binary[Float, Double, Double](x, y, _ % _)
    case (RuntimeType.Float, RuntimeType.Float) => Expr.Binary[Float, Float, Float](x, y, _ % _)
    case (RuntimeType.Float, RuntimeType.Int) => Expr.Binary[Float, Int, Float](x, y, _ % _)
    case (RuntimeType.Float, RuntimeType.Long) => Expr.Binary[Float, Long, Float](x, y, _ % _)
    case _ => throw new InvalidExpressionException("mod", x, y)

  def abs: Expr = x.typeOf match
    case RuntimeType.Long => Expr.Unary[Long, Long](x, math.abs)
    case RuntimeType.Int => Expr.Unary[Int, Int](x, math.abs)
    case RuntimeType.Double => Expr.Unary[Double, Double](x, math.abs)
    case RuntimeType.Float => Expr.Unary[Float, Float](x, math.abs)
    case _ => throw new InvalidExpressionException("abs", x)

  def negate: Expr = x.typeOf match
    case RuntimeType.Long => Expr.Unary[Long, Long](x, -_)
    case RuntimeType.Int => Expr.Unary[Int, Int](x, -_)
    case RuntimeType.Double => Expr.Unary[Double, Double](x, -_)
    case RuntimeType.Float => Expr.Unary[Float, Float](x, -_)
    case _ => throw new InvalidExpressionException("negate", x)
