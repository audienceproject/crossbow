# Crossbow

Single node, in-memory DataFrame analytics library.

* Pure Scala; 0 dependencies
* Specialized operations on primitive types
* Fluent expression DSL
* Immutable public API; `Array` under the hood

## News

* 2024-03-22: Upgraded to Scala 3 with version `0.2.0`. Some parts of the source code have been completely rewritten,
  but the feature set remains the same. The intention is for Crossbow to be a Scala 3 library going forward.

## Installing

The library is available through Maven Central.

SBT style dependency: `"com.audienceproject" %% "crossbow" % "latest"`

# API

```scala 3
import com.audienceproject.crossbow.{*, given}

val data = Seq(("a", 1), ("b", 2), ("c", 3))
val df = DataFrame.fromSeq(data)

df.printSchema()

/**
 * _0: string
 * _1: int
 */

df.as[(String, Int)].foreach(println)

/**
 * (a, 1)
 * (b, 2)
 * (c, 3)
 */
```

## Transforming

```scala 3
val df = Seq((1, 2), (3, 4)).toDataFrame("x", "y")
df.select($"x" + $"y" / 2d as "avg", ($"x", $"y") as "tuple")

// Lambda functions
val pythagoras = lambda[(Int, Int), Double]:
  (a, b) => math.sqrt(a * a + b * b)
df.select(pythagoras($"x", $"y"))
```

## Filtering

```scala 3
df.filter($"x" >= 2 && $"y" % 2 =!= 0)
```

## Grouping

```scala 3
val df = Seq(("foo", 1), ("foo", 2), ("bar", 3)).toDataFrame("someKey", "x")
df.groupBy($"someKey").agg(sum($"x") / count() as "avg", collect($"x") as "xs")

// Custom aggregators
val product = reducer[Int, Int](1)(_ * _)
df.groupBy($"someKey").agg(product($"x") as "product")
```

## Sorting

```scala 3
df.sortBy($"x")

// Sorting on 'x' first, then 'y'
df.sortBy(($"someKey", $"x"))

// Sorting with explicit ordering (e.g. integer descending)
df.sortBy($"x")(using Order.by(Ordering.Int.reverse))
```

## Joining

```scala 3
val otherDf = Seq(("foo", 1, 10d), ("foo", 2, 20d), ("bar", 3, 30d)).toDataFrame("someKey", "x", "y")

// Inner join
df.join(otherDf, $"someKey")

// Inner join on multiple columns
df.join(otherDf, ($"someKey", $"x"))

// Other join types
df.join(otherDf, $"someKey", JoinType.LeftOuter)
```
