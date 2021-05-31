2021-05-31
----------

* Make "Sum" of all-null return 0, not null.
* Fix error on "Sum" of empty table without groups.

2021-05-07
----------

* Make "Group Dates" suggest text=>timestamp quick fix. (This means two
  quick fixes -- text=>timestamp + timestamp=>date -- to complete one
  task. We hope users will be able to follow the logic.)

2021-05-05
----------

* Make "Group Dates" checkbox suggest quick fixes.

Up to now, Workbench has been making the "Group Dates" checkbox a special
case. It would suggest quick fixes _before the user clicked Go_. Now, the
quick-fix button behaves like any other quick fix.

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
