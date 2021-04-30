2021-04-30
----------

Rewrite in Arrow. Advantages:

* Fix crash selecting FIRST of dictionary-encoded text
* Preserve output column formatting: sum+min+max+first of numbers will have
  the same formats as the input.
* Preserve 64-bit integers: sum+min+max+first+nunique don't convert to float
  any more. (We no longer convert to Numpy: Numpy i64 arrays can't hold null.)
* Speedup: in a 123MB, 150k-row, 66-group example, wall time dropped from
  8,800ms to 111ms. Probably because we use simple sorts instead of hash
  tables; and data never enters pure-Python land.

2021-01-13
----------

* When grouping by Week/Quarter, ignore nulls

2020-09-30
----------

* Allow group by "Week" (ISO-8601 weeks, UTC)
