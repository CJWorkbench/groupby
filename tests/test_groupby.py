from datetime import datetime as dt

import pyarrow as pa
from cjwmodule.arrow.testing import assert_arrow_table_equals, make_column, make_table

from groupby import Aggregation, DateGranularity, Group, Operation, groupby


def test_no_colnames():
    assert_arrow_table_equals(
        groupby(
            make_table(make_column("A", [1, 2])),
            [],
            [Aggregation(Operation.SUM, "A", "X")],
        ),
        make_table(make_column("X", [3])),
    )


def test_size():
    assert_arrow_table_equals(
        groupby(
            make_table(make_column("A", [1, 1, 2])),
            [Group("A", None)],
            [Aggregation(Operation.SIZE, "", "X")],
        ),
        make_table(make_column("A", [1, 2]), make_column("X", [2, 1], format="{:,d}")),
    )


def test_multilevel():
    assert_arrow_table_equals(
        groupby(
            make_table(
                make_column("A", [1, 1, 1, 2]),
                make_column("B", [1, 1, 2, 2]),
                make_column("C", [0, 1, -1, 0]),
            ),
            [Group("A", None), Group("B", None)],
            [Aggregation(Operation.SUM, "C", "D")],
        ),
        make_table(
            make_column("A", [1, 1, 2]),
            make_column("B", [1, 2, 2]),
            make_column("D", [1, -1, 0]),
        ),
    )


def test_multilevel_with_na_remove_unused_category():
    assert_arrow_table_equals(
        groupby(
            make_table(
                make_column("A", ["a1", "a2", "a1", "a1"], dictionary=True),
                make_column("B", ["b1", None, "b2", "b3"], dictionary=True),
            ),
            [Group("A", None), Group("B", None)],
            [Aggregation(Operation.SIZE, "", "X")],
        ),
        make_table(
            make_column("A", ["a1", "a1", "a1"], dictionary=True),
            make_column("B", ["b1", "b2", "b3"]),
            make_column("X", [1, 1, 1], format="{:,d}"),
        ),
    )


def test_do_not_multiply_categories():
    # Pandas default, when given categoricals, is to multiply them out:
    # in this example, we'd get four rows:
    #
    #     a, c
    #     a, d
    #     b, c
    #     b, d
    #
    # ... even though there are no values for (a, d) or (b, c).
    #
    # See https://github.com/pandas-dev/pandas/issues/17594. The solution
    # is .groupby(..., observed=True).
    assert_arrow_table_equals(
        groupby(
            make_table(
                make_column("A", ["a", "b", "a"], dictionary=True),
                make_column("B", ["c", "d", "d"], dictionary=True),
                make_column("C", [1, 2, 3]),
            ),
            [Group("A", None), Group("B", None)],
            [Aggregation(Operation.SUM, "C", "X")],
        ),
        make_table(
            make_column("A", ["a", "a", "b"], dictionary=True),
            make_column("B", ["c", "d", "d"], dictionary=True),
            make_column("X", [1, 3, 2]),
        ),
    )


def test_allow_duplicate_aggregations():
    assert_arrow_table_equals(
        groupby(
            make_table(
                make_column("A", [1, 1, 2]),
                make_column("B", [1, 2, 3]),
            ),
            [Group("A", None)],
            [
                Aggregation(Operation.MIN, "B", "X"),
                Aggregation(Operation.MIN, "B", "Y"),
            ],
        ),
        make_table(
            make_column("A", [1, 2]), make_column("X", [1, 3]), make_column("Y", [1, 3])
        ),
    )


def test_aggregate_grouped_column():
    # Does the user know what he or she is doing? Dunno. But
    # [adamhooper, 2019-01-03] SQL would aggregate the single
    # value, so why shouldn't Workbench?
    assert_arrow_table_equals(
        groupby(
            make_table(
                make_column("A", [1, 1, 2]),
                make_column("B", [1, 2, 2]),
                make_column("C", [5, 5, 5]),
            ),
            [Group("A", None), Group("B", None)],
            [Aggregation(Operation.SUM, "B", "X")],
        ),
        make_table(
            make_column("A", [1, 1, 2]),
            make_column("B", [1, 2, 2]),
            make_column("X", [1, 2, 2]),  # just a copy of B
        ),
    )


def test_aggregate_numbers():
    assert_arrow_table_equals(
        groupby(
            make_table(
                make_column("A", [2, 1, 2, 2], format="{:.2f}"),
                make_column("B", [1, 2, 5, 1], format="{:d}"),
            ),
            [Group("A", None)],
            [
                Aggregation(Operation.SIZE, "", "size"),
                Aggregation(Operation.NUNIQUE, "B", "nunique"),
                Aggregation(Operation.SUM, "B", "sum"),
                Aggregation(Operation.MEAN, "B", "mean"),
                Aggregation(Operation.MEDIAN, "B", "median"),
                Aggregation(Operation.MIN, "B", "min"),
                Aggregation(Operation.MAX, "B", "max"),
                Aggregation(Operation.FIRST, "B", "first"),
            ],
        ),
        make_table(
            make_column("A", [1, 2], format="{:.2f}"),  # format from A
            make_column("size", [1, 3], format="{:,d}"),  # int format
            make_column("nunique", [1, 2], format="{:,d}"),  # int format
            make_column("sum", [2, 7], format="{:d}"),  # format from B
            make_column("mean", [2, 7 / 3], format="{:,}"),  # default format
            make_column("median", [2.0, 1.0], format="{:,}"),  # default format
            make_column("min", [2, 1], format="{:d}"),  # format from B
            make_column("max", [2, 5], format="{:d}"),  # format from B
            make_column("first", [2, 1], format="{:d}"),  # format from B
        ),
    )


def test_aggregate_numbers_all_nulls():
    assert_arrow_table_equals(
        groupby(
            make_table(
                make_column("A", [1], format="{:.2f}"),
                make_column("B", [None], pa.int32(), format="{:d}"),
            ),
            [Group("A", None)],
            [
                Aggregation(Operation.SIZE, "", "size"),
                Aggregation(Operation.NUNIQUE, "B", "nunique"),
                Aggregation(Operation.SUM, "B", "sum"),
                Aggregation(Operation.MEAN, "B", "mean"),
                Aggregation(Operation.MEDIAN, "B", "median"),
                Aggregation(Operation.MIN, "B", "min"),
                Aggregation(Operation.MAX, "B", "max"),
                Aggregation(Operation.FIRST, "B", "first"),
            ],
        ),
        make_table(
            make_column("A", [1], format="{:.2f}"),  # format from A
            make_column("size", [1], pa.int64(), format="{:,d}"),  # int format
            make_column("nunique", [0], pa.int64(), format="{:,d}"),  # int format
            # TODO make "sum" int64
            make_column("sum", [0], pa.int32(), format="{:d}"),  # format from B
            make_column("mean", [None], pa.float64(), format="{:,}"),  # default format
            make_column(
                "median", [None], pa.float64(), format="{:,}"
            ),  # default format
            make_column("min", [None], pa.int32(), format="{:d}"),  # format from B
            make_column("max", [None], pa.int32(), format="{:d}"),  # format from B
            make_column("first", [None], pa.int32(), format="{:d}"),  # format from B
        ),
    )


def test_aggregate_numbers_no_groups():
    assert_arrow_table_equals(
        groupby(
            make_table(
                make_column("A", [], pa.int16(), format="{:.2f}"),
                make_column("B", [], pa.int32(), format="{:d}"),
            ),
            [Group("A", None)],
            [
                Aggregation(Operation.SIZE, "", "size"),
                Aggregation(Operation.NUNIQUE, "B", "nunique"),
                Aggregation(Operation.SUM, "B", "sum"),
                Aggregation(Operation.MEAN, "B", "mean"),
                Aggregation(Operation.MEDIAN, "B", "median"),
                Aggregation(Operation.MIN, "B", "min"),
                Aggregation(Operation.MAX, "B", "max"),
                Aggregation(Operation.FIRST, "B", "first"),
            ],
        ),
        make_table(
            make_column("A", [], pa.int16(), format="{:.2f}"),  # format from A
            make_column("size", [], pa.int64(), format="{:,d}"),  # int format
            make_column("nunique", [], pa.int64(), format="{:,d}"),  # int format
            # TODO make "sum" int64
            make_column("sum", [], pa.int32(), format="{:d}"),  # format from B
            make_column("mean", [], pa.float64(), format="{:,}"),  # default format
            make_column("median", [], pa.float64(), format="{:,}"),  # default format
            make_column("min", [], pa.int32(), format="{:d}"),  # format from B
            make_column("max", [], pa.int32(), format="{:d}"),  # format from B
            make_column("first", [], pa.int32(), format="{:d}"),  # format from B
        ),
    )


def test_aggregate_text_values():
    assert_arrow_table_equals(
        groupby(
            make_table(
                make_column("A", [1, 1, 1]),
                make_column("B", ["a", "b", "a"]),
            ),
            [Group("A", None)],
            [
                Aggregation(Operation.SIZE, "B", "size"),
                Aggregation(Operation.NUNIQUE, "B", "nunique"),
                Aggregation(Operation.MIN, "B", "min"),
                Aggregation(Operation.MAX, "B", "max"),
                Aggregation(Operation.FIRST, "B", "first"),
            ],
        ),
        make_table(
            make_column("A", [1]),
            make_column("size", [3], format="{:,d}"),
            make_column("nunique", [2], format="{:,d}"),
            make_column("min", ["a"]),
            make_column("max", ["b"]),
            make_column("first", ["a"]),
        ),
    )


def test_aggregate_text_category_values():
    assert_arrow_table_equals(
        groupby(
            make_table(
                make_column("A", [1, 1, 1]),
                make_column("B", ["a", "b", "a"], dictionary=True),
            ),
            [Group("A", None)],
            [
                Aggregation(Operation.SIZE, "B", "size"),
                Aggregation(Operation.NUNIQUE, "B", "nunique"),
                Aggregation(Operation.MIN, "B", "min"),
                Aggregation(Operation.MAX, "B", "max"),
                Aggregation(Operation.FIRST, "B", "first"),
            ],
        ),
        make_table(
            make_column("A", [1]),
            make_column("size", [3], format="{:,d}"),
            make_column("nunique", [2], format="{:,d}"),
            make_column("min", ["a"], dictionary=True),
            make_column("max", ["b"], dictionary=True),
            make_column("first", ["a"], dictionary=True),
        ),
    )


def test_aggregate_text_category_values_max():
    # https://github.com/pandas-dev/pandas/issues/28641
    assert_arrow_table_equals(
        groupby(
            make_table(
                make_column("A", [1997]),
                make_column("B", ["30-SEP-97"], dictionary=True),
            ),
            [Group("A", None)],
            [Aggregation(Operation.MAX, "B", "X")],
        ),
        make_table(
            make_column("A", [1997]), make_column("X", ["30-SEP-97"], dictionary=True)
        ),
    )


def test_aggregate_text_category_values_with_multiple_agg_columns():
    # Typo uncovered [2019-11-14] meant we'd only call .as_ordered() when
    # the _last_ column was categorical (as opposed to when the
    # _categorical_ column was categorical).
    assert_arrow_table_equals(
        groupby(
            make_table(
                make_column("A", [1997]), make_column("B", ["b"], dictionary=True)
            ),
            [Group("A", None)],
            [
                Aggregation(Operation.MAX, "B", "X"),
                Aggregation(Operation.MIN, "A", "Y"),
            ],
        ),
        make_table(
            make_column("A", [1997]),
            make_column("X", ["b"], dictionary=True),
            make_column("Y", [1997]),
        ),
    )


def test_first_in_category():
    # https://www.pivotaltracker.com/story/show/177964511
    # This crash finally inspired us, [2021-04-29], to ditch Pandas.
    #
    # The only shock is that we didn't ditch it after all the other crashes
    # that litter this test suite.
    assert_arrow_table_equals(
        groupby(
            make_table(
                make_column("A", ["A", "A"], dictionary=True),
            ),
            [],
            [Aggregation(Operation.FIRST, "A", "first")],
        ),
        make_table(make_column("first", ["A"], dictionary=True)),
    )


def test_aggregate_text_category_values_empty_is_still_str():
    assert_arrow_table_equals(
        groupby(
            make_table(make_column("A", [None], pa.utf8(), dictionary=True)),
            [Group("A", None)],
            [
                Aggregation(Operation.SIZE, "", "size"),
                Aggregation(Operation.NUNIQUE, "A", "nunique"),
                Aggregation(Operation.MIN, "A", "min"),
                Aggregation(Operation.MAX, "A", "max"),
                Aggregation(Operation.FIRST, "A", "first"),
            ],
        ),
        make_table(
            make_column("A", [], pa.utf8(), dictionary=True),
            make_column("size", [], pa.int64(), format="{:,d}"),
            make_column("nunique", [], pa.int64(), format="{:,d}"),
            make_column("min", [], pa.utf8(), dictionary=True),
            make_column("max", [], pa.utf8(), dictionary=True),
            make_column("first", [], pa.utf8(), dictionary=True),
        ),
    )


def test_aggregate_timestamp():
    assert_arrow_table_equals(
        groupby(
            make_table(
                make_column("A", [dt(2018, 1, 4), dt(2018, 1, 5), dt(2018, 1, 4)]),
            ),
            [Group("A", None)],
            [Aggregation(Operation.SIZE, "", "size")],
        ),
        make_table(
            make_column("A", [dt(2018, 1, 4), dt(2018, 1, 5)]),
            make_column("size", [2, 1], format="{:,d}"),
        ),
    )


def test_aggregate_timestamp_by_second_DEPRECATED():
    assert_arrow_table_equals(
        groupby(
            make_table(
                make_column(
                    "A",
                    [
                        dt(2018, 1, 4, 1, 2, 3, 200),
                        dt(2018, 1, 4, 1, 2, 7, 432),
                        dt(2018, 1, 4, 1, 2, 3, 123),
                    ],
                )
            ),
            [Group("A", DateGranularity.SECOND)],
            [Aggregation(Operation.SIZE, "", "size")],
        ),
        make_table(
            make_column("A", [dt(2018, 1, 4, 1, 2, 3), dt(2018, 1, 4, 1, 2, 7)]),
            make_column("size", [2, 1], format="{:,d}"),
        ),
    )


def test_aggregate_timestamp_by_minute_DEPRECATED():
    assert_arrow_table_equals(
        groupby(
            make_table(
                make_column(
                    "A",
                    [
                        dt(2018, 1, 4, 1, 2, 3),
                        dt(2018, 1, 4, 1, 8, 3),
                        dt(2018, 1, 4, 1, 2, 8),
                    ],
                )
            ),
            [Group("A", DateGranularity.MINUTE)],
            [Aggregation(Operation.SIZE, "", "size")],
        ),
        make_table(
            make_column("A", [dt(2018, 1, 4, 1, 2), dt(2018, 1, 4, 1, 8)]),
            make_column("size", [2, 1], format="{:,d}"),
        ),
    )


def test_aggregate_timestamp_by_hour_DEPRECATED():
    assert_arrow_table_equals(
        groupby(
            make_table(
                make_column(
                    "A",
                    [
                        dt(2018, 1, 4, 1, 2, 3),
                        dt(2018, 1, 4, 2, 2, 3),
                        dt(2018, 1, 4, 1, 8, 3),
                    ],
                )
            ),
            [Group("A", DateGranularity.HOUR)],
            [Aggregation(Operation.SIZE, "", "size")],
        ),
        make_table(
            make_column("A", [dt(2018, 1, 4, 1), dt(2018, 1, 4, 2)]),
            make_column("size", [2, 1], format="{:,d}"),
        ),
    )


def test_aggregate_timestamp_by_day_DEPRECATED():
    assert_arrow_table_equals(
        groupby(
            make_table(
                make_column(
                    "A",
                    [
                        dt(2018, 1, 4, 1, 2, 3),
                        dt(2018, 2, 4, 1, 2, 3),
                        dt(2018, 1, 4, 1, 2, 3),
                    ],
                )
            ),
            [Group("A", DateGranularity.DAY)],
            [Aggregation(Operation.SIZE, "", "size")],
        ),
        make_table(
            make_column("A", [dt(2018, 1, 4), dt(2018, 2, 4)]),
            make_column("size", [2, 1], format="{:,d}"),
        ),
    )


def test_aggregate_timestamp_by_week_DEPRECATED():
    assert_arrow_table_equals(
        groupby(
            make_table(
                make_column("A", [dt(2018, 1, 4), dt(2018, 2, 4), dt(2018, 1, 6)])
            ),
            [Group("A", DateGranularity.WEEK)],
            [Aggregation(Operation.SIZE, "", "size")],
        ),
        make_table(
            make_column("A", [dt(2018, 1, 1), dt(2018, 1, 29)]),
            make_column("size", [2, 1], format="{:,d}"),
        ),
    )


def test_aggregate_null_timestamp_by_week_DEPRECATED():
    assert_arrow_table_equals(
        groupby(
            make_table(make_column("A", [None], pa.timestamp("ns"))),
            [Group("A", DateGranularity.WEEK)],
            [Aggregation(Operation.SIZE, "", "size")],
        ),
        make_table(
            make_column("A", [], pa.timestamp("ns")),
            make_column("size", [], pa.int64(), format="{:,d}"),
        ),
    )


def test_aggregate_timestamp_by_month_DEPRECATED():
    assert_arrow_table_equals(
        groupby(
            make_table(
                make_column("A", [dt(2018, 1, 4), dt(2018, 2, 4), dt(2018, 1, 6)])
            ),
            [Group("A", DateGranularity.MONTH)],
            [Aggregation(Operation.SIZE, "", "size")],
        ),
        make_table(
            make_column("A", [dt(2018, 1, 1), dt(2018, 2, 1)]),
            make_column("size", [2, 1], format="{:,d}"),
        ),
    )


def test_aggregate_timestamp_by_quarter_DEPRECATED():
    assert_arrow_table_equals(
        groupby(
            make_table(
                make_column("A", [dt(2018, 1, 4), dt(2018, 6, 4), dt(2018, 3, 4)]),
            ),
            [Group("A", DateGranularity.QUARTER)],
            [Aggregation(Operation.SIZE, "", "size")],
        ),
        make_table(
            make_column("A", [dt(2018, 1, 1), dt(2018, 4, 1)]),
            make_column("size", [2, 1], format="{:,d}"),
        ),
    )


def test_aggregate_null_timestamp_by_quarter_DEPRECATED():
    assert_arrow_table_equals(
        groupby(
            make_table(make_column("A", [None], pa.timestamp("ns"))),
            [Group("A", DateGranularity.QUARTER)],
            [Aggregation(Operation.SIZE, "", "size")],
        ),
        make_table(
            make_column("A", [], pa.timestamp("ns")),
            make_column("size", [], pa.int64(), format="{:,d}"),
        ),
    )


def test_aggregate_timestamp_by_year_DEPRECATED():
    assert_arrow_table_equals(
        groupby(
            make_table(
                make_column("A", [dt(2018, 1, 4), dt(2019, 2, 4), dt(2018, 3, 4)]),
            ),
            [Group("A", DateGranularity.YEAR)],
            [Aggregation(Operation.SIZE, "", "size")],
        ),
        make_table(
            make_column("A", [dt(2018, 1, 1), dt(2019, 1, 1)]),
            make_column("size", [2, 1], format="{:,d}"),
        ),
    )


def test_zero_chunks_into_groups():
    assert_arrow_table_equals(
        groupby(
            pa.table(
                [pa.chunked_array([], pa.utf8()), pa.chunked_array([], pa.int32())],
                schema=pa.schema(
                    [
                        pa.field("text", pa.utf8()),
                        pa.field("number", pa.int64(), metadata={"format": "{:,.2f}"}),
                    ]
                ),
            ),
            [Group("text", None)],
            [
                Aggregation(Operation.FIRST, "number", "first"),
                Aggregation(Operation.MAX, "text", "max"),
                Aggregation(Operation.MEAN, "number", "mean"),
                Aggregation(Operation.MEDIAN, "number", "median"),
                Aggregation(Operation.MIN, "number", "min"),
                Aggregation(Operation.NUNIQUE, "text", "nunique"),
                Aggregation(Operation.SIZE, "", "size"),
                Aggregation(Operation.SUM, "number", "sum"),
            ],
        ),
        make_table(
            make_column("text", [], pa.utf8()),
            make_column("first", [], pa.int64(), format="{:,.2f}"),
            make_column("max", [], pa.utf8()),
            make_column("mean", [], pa.float64(), format="{:,}"),
            make_column("median", [], pa.float64(), format="{:,}"),
            make_column("min", [], pa.int64(), format="{:,.2f}"),
            make_column("nunique", [], pa.int64(), format="{:,d}"),
            make_column("size", [], pa.int64(), format="{:,d}"),
            make_column("sum", [], pa.int64(), format="{:,.2f}"),
        ),
    )


def test_zero_chunks_functions():
    # https://www.pivotaltracker.com/story/show/178332598
    assert_arrow_table_equals(
        groupby(
            pa.table(
                [pa.chunked_array([], pa.utf8()), pa.chunked_array([], pa.int32())],
                schema=pa.schema(
                    [
                        pa.field("text", pa.utf8()),
                        pa.field("number", pa.int64(), metadata={"format": "{:,.2f}"}),
                    ]
                ),
            ),
            [],
            [
                Aggregation(Operation.FIRST, "number", "first"),
                Aggregation(Operation.MAX, "text", "max"),
                Aggregation(Operation.MEAN, "number", "mean"),
                Aggregation(Operation.MEDIAN, "number", "median"),
                Aggregation(Operation.MIN, "number", "min"),
                Aggregation(Operation.NUNIQUE, "text", "nunique"),
                Aggregation(Operation.SIZE, "", "size"),
                Aggregation(Operation.SUM, "number", "sum"),
            ],
        ),
        make_table(
            make_column("first", [None], pa.int64(), format="{:,.2f}"),
            make_column("max", [None], pa.utf8()),
            make_column("mean", [None], pa.float64(), format="{:,}"),
            make_column("median", [None], pa.float64(), format="{:,}"),
            make_column("min", [None], pa.int64(), format="{:,.2f}"),
            make_column("nunique", [0], pa.int64(), format="{:,d}"),
            make_column("size", [0], pa.int64(), format="{:,d}"),
            make_column("sum", [0], pa.int64(), format="{:,.2f}"),
        ),
    )
