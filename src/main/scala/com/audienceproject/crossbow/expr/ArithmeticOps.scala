package com.audienceproject.crossbow.expr

trait ArithmeticOps {

  self: Expr =>

  def +(other: Expr): Expr = ArithmeticOps.Plus(this, other)

  def -(other: Expr): Expr = ArithmeticOps.Minus(this, other)

  def *(other: Expr): Expr = ArithmeticOps.Multiply(this, other)

  def /(other: Expr): Expr = ArithmeticOps.Divide(this, other)

  def %(other: Expr): Expr = ArithmeticOps.Mod(this, other)

  def abs(): Expr = ArithmeticOps.Abs(this)

  def negate(): Expr = ArithmeticOps.Negate(this)

}

private object ArithmeticOps {

  case class Plus(lhs: Expr, rhs: Expr) extends BinaryExpr(lhs, rhs) {
    override def typeSpec(lhsOperand: Specialized[_], rhsOperand: Specialized[_]): Specialized[_] =
      (lhsOperand.getType, rhsOperand.getType) match {
        // Long
        case ("long", "long") => specialize[Long, Long, Long](lhsOperand, rhsOperand, _ + _)
        case ("long", "int") => specialize[Long, Int, Long](lhsOperand, rhsOperand, _ + _)
        case ("int", "long") => specialize[Int, Long, Long](lhsOperand, rhsOperand, _ + _)
        // Int
        case ("int", "int") => specialize[Int, Int, Int](lhsOperand, rhsOperand, _ + _)
        // Double
        case ("double", "double") => specialize[Double, Double, Double](lhsOperand, rhsOperand, _ + _)
        case ("double", "long") => specialize[Double, Long, Double](lhsOperand, rhsOperand, _ + _)
        case ("long", "double") => specialize[Long, Double, Double](lhsOperand, rhsOperand, _ + _)
        case ("double", "int") => specialize[Double, Int, Double](lhsOperand, rhsOperand, _ + _)
        case ("int", "double") => specialize[Int, Double, Double](lhsOperand, rhsOperand, _ + _)
        case _ => throw new InvalidExpressionException("Plus", lhsOperand.getType, rhsOperand.getType)
      }
  }

  case class Minus(lhs: Expr, rhs: Expr) extends BinaryExpr(lhs, rhs) {
    override def typeSpec(lhsOperand: Specialized[_], rhsOperand: Specialized[_]): Specialized[_] =
      (lhsOperand.getType, rhsOperand.getType) match {
        // Long
        case ("long", "long") => specialize[Long, Long, Long](lhsOperand, rhsOperand, _ - _)
        case ("long", "int") => specialize[Long, Int, Long](lhsOperand, rhsOperand, _ - _)
        case ("int", "long") => specialize[Int, Long, Long](lhsOperand, rhsOperand, _ - _)
        // Int
        case ("int", "int") => specialize[Int, Int, Int](lhsOperand, rhsOperand, _ - _)
        // Double
        case ("double", "double") => specialize[Double, Double, Double](lhsOperand, rhsOperand, _ - _)
        case ("double", "long") => specialize[Double, Long, Double](lhsOperand, rhsOperand, _ - _)
        case ("long", "double") => specialize[Long, Double, Double](lhsOperand, rhsOperand, _ - _)
        case ("double", "int") => specialize[Double, Int, Double](lhsOperand, rhsOperand, _ - _)
        case ("int", "double") => specialize[Int, Double, Double](lhsOperand, rhsOperand, _ - _)
        case _ => throw new InvalidExpressionException("Minus", lhsOperand.getType, rhsOperand.getType)
      }
  }

  case class Multiply(lhs: Expr, rhs: Expr) extends BinaryExpr(lhs, rhs) {
    override def typeSpec(lhsOperand: Specialized[_], rhsOperand: Specialized[_]): Specialized[_] =
      (lhsOperand.getType, rhsOperand.getType) match {
        // Long
        case ("long", "long") => specialize[Long, Long, Long](lhsOperand, rhsOperand, _ * _)
        case ("long", "int") => specialize[Long, Int, Long](lhsOperand, rhsOperand, _ * _)
        case ("int", "long") => specialize[Int, Long, Long](lhsOperand, rhsOperand, _ * _)
        // Int
        case ("int", "int") => specialize[Int, Int, Int](lhsOperand, rhsOperand, _ * _)
        // Double
        case ("double", "double") => specialize[Double, Double, Double](lhsOperand, rhsOperand, _ * _)
        case ("double", "long") => specialize[Double, Long, Double](lhsOperand, rhsOperand, _ * _)
        case ("long", "double") => specialize[Long, Double, Double](lhsOperand, rhsOperand, _ * _)
        case ("double", "int") => specialize[Double, Int, Double](lhsOperand, rhsOperand, _ * _)
        case ("int", "double") => specialize[Int, Double, Double](lhsOperand, rhsOperand, _ * _)
        case _ => throw new InvalidExpressionException("Multiply", lhsOperand.getType, rhsOperand.getType)
      }
  }

  case class Divide(lhs: Expr, rhs: Expr) extends BinaryExpr(lhs, rhs) {
    override def typeSpec(lhsOperand: Specialized[_], rhsOperand: Specialized[_]): Specialized[_] =
      (lhsOperand.getType, rhsOperand.getType) match {
        // Long
        case ("long", "long") => specialize[Long, Long, Long](lhsOperand, rhsOperand, _ / _)
        case ("long", "int") => specialize[Long, Int, Long](lhsOperand, rhsOperand, _ / _)
        case ("int", "long") => specialize[Int, Long, Long](lhsOperand, rhsOperand, _ / _)
        // Int
        case ("int", "int") => specialize[Int, Int, Int](lhsOperand, rhsOperand, _ / _)
        // Double
        case ("double", "double") => specialize[Double, Double, Double](lhsOperand, rhsOperand, _ / _)
        case ("double", "long") => specialize[Double, Long, Double](lhsOperand, rhsOperand, _ / _)
        case ("long", "double") => specialize[Long, Double, Double](lhsOperand, rhsOperand, _ / _)
        case ("double", "int") => specialize[Double, Int, Double](lhsOperand, rhsOperand, _ / _)
        case ("int", "double") => specialize[Int, Double, Double](lhsOperand, rhsOperand, _ / _)
        case _ => throw new InvalidExpressionException("Divide", lhsOperand.getType, rhsOperand.getType)
      }
  }

  case class Mod(lhs: Expr, rhs: Expr) extends BinaryExpr(lhs, rhs) {
    override def typeSpec(lhsOperand: Specialized[_], rhsOperand: Specialized[_]): Specialized[_] =
      (lhsOperand.getType, rhsOperand.getType) match {
        // Long
        case ("long", "long") => specialize[Long, Long, Long](lhsOperand, rhsOperand, _ % _)
        case ("long", "int") => specialize[Long, Int, Long](lhsOperand, rhsOperand, _ % _)
        case ("int", "long") => specialize[Int, Long, Long](lhsOperand, rhsOperand, _ % _)
        // Int
        case ("int", "int") => specialize[Int, Int, Int](lhsOperand, rhsOperand, _ % _)
        // Double
        case ("double", "double") => specialize[Double, Double, Double](lhsOperand, rhsOperand, _ % _)
        case ("double", "long") => specialize[Double, Long, Double](lhsOperand, rhsOperand, _ % _)
        case ("long", "double") => specialize[Long, Double, Double](lhsOperand, rhsOperand, _ % _)
        case ("double", "int") => specialize[Double, Int, Double](lhsOperand, rhsOperand, _ % _)
        case ("int", "double") => specialize[Int, Double, Double](lhsOperand, rhsOperand, _ % _)
        case _ => throw new InvalidExpressionException("Mod", lhsOperand.getType, rhsOperand.getType)
      }
  }

  case class Abs(expr: Expr) extends UnaryExpr(expr) {
    override def typeSpec(operand: Specialized[_]): Specialized[_] = operand.getType match {
      case "long" => specialize[Long, Long](operand, math.abs)
      case "int" => specialize[Int, Int](operand, math.abs)
      case "double" => specialize[Double, Double](operand, math.abs)
      case _ => throw new InvalidExpressionException("Abs", operand.getType)
    }
  }

  case class Negate(expr: Expr) extends UnaryExpr(expr) {
    override def typeSpec(operand: Specialized[_]): Specialized[_] = operand.getType match {
      case "long" => specialize[Long, Long](operand, math.negateExact)
      case "int" => specialize[Int, Int](operand, math.negateExact)
      case "double" => specialize[Double, Double](operand, -_)
      case _ => throw new InvalidExpressionException("Negate", operand.getType)
    }
  }

}
