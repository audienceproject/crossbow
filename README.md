# Crossbow

Single node, in-memory DataFrame analytics library.

* Pure Scala; 0 dependencies
* Specialized operations on primitive types
* Fluent expression DSL
* Immutable public API; `Array` under the hood

## Installing
The library is available through Maven Central.

SBT style dependency: `"com.audienceproject" %% "crossbow" % "0.1.0"`

# API
```scala
import com.audienceproject.crossbow.DataFrame
import com.audienceproject.crossbow.Implicits._

val data = Seq(("a", 1), ("b", 2), ("c", 3))
val df = DataFrame.fromSeq(data)

df.printSchema()
/**
 * _0: String
 * _1: int
 */

df.as[(String, Int)].foreach(println)
/**
 * ("a", 1)
 * ("b", 2)
 * ("c", 3)
 */
```

## Transforming
```scala
df.select($"x" + $"y" / 2d as "avg", ($"x", $"y") as "tuple")

// Lambda functions
val toUpper = lambda[String, String](_.toUpperCase)
df.select(toUpper($"a") as "upperCaseA")
```

## Filtering
```scala
df.filter($"x" >= 2 && $"y" % 2 =!= 0)
```

## Grouping
```scala
df.groupBy($"someKey").agg(sum($"x") / count($"x") as "avg", collect($"x") as "xs")

// Custom aggregators
val product = reducer[Int, Int](1)(_ * _)
df.groupBy($"someKey").agg(product($"x") as "product")
```

## Sorting
```scala
df.sortBy($"x")

// Sorting on 'x' first, then 'y'
df.sortBy(($"x", $"y"))

// Sorting with explicit ordering (e.g. integer descending)
import com.audienceproject.crossbow.expr.Order
df.sortBy($"x", Order.by(Ordering.Int.reverse))
```

## Joining
```scala
// Inner join
df.join(otherDf, $"someKey")

// Inner join on multiple columns
df.join(otherDf, ($"key1", $"key2"))

// Other join types
import com.audienceproject.crossbow.JoinType
df.join(otherDf, $"someKey", JoinType.LeftOuter)
```