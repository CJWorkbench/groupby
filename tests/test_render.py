import unittest
from datetime import datetime as dt

import numpy as np
import pandas as pd
from cjwmodule.testing.i18n import i18n_message
from pandas.testing import assert_frame_equal

from groupby import (
    Aggregation,
    DateGranularity,
    Group,
    Operation,
    groupby,
    render,
)


class GroupbyTest(unittest.TestCase):
    def test_no_colnames(self):
        table = pd.DataFrame({"A": [1, 2]})
        result = groupby(table, [], [Aggregation(Operation.SUM, "A", "X")])
        assert_frame_equal(result, pd.DataFrame({"X": [3]}))

    def test_size(self):
        table = pd.DataFrame({"A": [1, 1, 2]})
        result = groupby(
            table, [Group("A", None)], [Aggregation(Operation.SIZE, "", "X")]
        )
        assert_frame_equal(result, pd.DataFrame({"A": [1, 2], "X": [2, 1]}))

    def test_multilevel(self):
        result = groupby(
            pd.DataFrame({"A": [1, 1, 1, 2], "B": [1, 1, 2, 2], "C": [0, 1, -1, 0]}),
            [Group("A", None), Group("B", None)],
            [Aggregation(Operation.SUM, "C", "D")],
        )
        assert_frame_equal(
            result, pd.DataFrame({"A": [1, 1, 2], "B": [1, 2, 2], "D": [1, -1, 0]})
        )

    def test_multilevel_with_na_remove_unused_category(self):
        result = groupby(
            pd.DataFrame({"A": ["a1", "a2"], "B": ["b1", np.nan]}, dtype="category"),
            [Group("A", None), Group("B", None)],
            [Aggregation(Operation.SIZE, "", "X")],
        )
        assert_frame_equal(
            result,
            pd.DataFrame(
                {
                    "A": pd.Series(["a1"], dtype="category"),
                    "B": pd.Series(["b1"], dtype="category"),
                    "X": [1],
                }
            ),
        )

    def test_do_not_multiply_categories(self):
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
        result = groupby(
            pd.DataFrame(
                {
                    "A": pd.Series(["a", "b"], dtype="category"),
                    "B": pd.Series(["c", "d"], dtype="category"),
                    "C": [1, 2],
                }
            ),
            [Group("A", None), Group("B", None)],
            [Aggregation(Operation.SUM, "C", "X")],
        )
        assert_frame_equal(
            result,
            pd.DataFrame(
                {
                    "A": pd.Series(["a", "b"], dtype="category"),
                    "B": pd.Series(["c", "d"], dtype="category"),
                    "X": [1, 2],
                }
            ),
        )

    def test_allow_duplicate_aggregations(self):
        result = groupby(
            pd.DataFrame({"A": [1, 1, 2], "B": [1, 2, 3]}),
            [Group("A", None)],
            [
                Aggregation(Operation.MIN, "B", "X"),
                Aggregation(Operation.MIN, "B", "Y"),
            ],
        )
        assert_frame_equal(
            result, pd.DataFrame({"A": [1, 2], "X": [1, 3], "Y": [1, 3]})
        )

    def test_aggregate_grouped_column(self):
        # Does the user know what he or she is doing? Dunno. But
        # [adamhooper, 2019-01-03] SQL would aggregate the single
        # value, so why shouldn't Workbench?
        result = groupby(
            pd.DataFrame({"A": [1, 1, 2], "B": [1, 2, 2], "C": [5, 5, 5]}),
            [Group("A", None), Group("B", None)],
            [Aggregation(Operation.SUM, "B", "X")],
        )
        assert_frame_equal(
            result,
            pd.DataFrame(
                {"A": [1, 1, 2], "B": [1, 2, 2], "X": [1, 2, 2]}  # just a copy of B
            ),
        )

    def test_aggregate_numbers(self):
        result = groupby(
            pd.DataFrame({"A": [2, 1, 2, 2], "B": [1, 2, 5, 1]}),
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
        )
        assert_frame_equal(
            result,
            pd.DataFrame(
                {
                    "A": [1, 2],
                    "size": [1, 3],
                    "nunique": [1, 2],
                    "sum": [2, 7],
                    "mean": [2, 7 / 3],
                    "median": [2, 1],
                    "min": [2, 1],
                    "max": [2, 5],
                    "first": [2, 1],
                }
            ),
        )

    def test_aggregate_text_values(self):
        result = groupby(
            pd.DataFrame({"A": [1, 1, 1], "B": ["a", "b", "a"]}),
            [Group("A", None)],
            [
                Aggregation(Operation.SIZE, "B", "size"),
                Aggregation(Operation.NUNIQUE, "B", "nunique"),
                Aggregation(Operation.MIN, "B", "min"),
                Aggregation(Operation.MAX, "B", "max"),
                Aggregation(Operation.FIRST, "B", "first"),
            ],
        )
        assert_frame_equal(
            result,
            pd.DataFrame(
                {
                    "A": [1],
                    "size": [3],
                    "nunique": [2],
                    "min": ["a"],
                    "max": ["b"],
                    "first": ["a"],
                }
            ),
        )

    def test_aggregate_text_category_values(self):
        result = groupby(
            pd.DataFrame(
                {"A": [1, 1, 1], "B": pd.Series(["a", "b", "a"], dtype="category")}
            ),
            [Group("A", None)],
            [
                Aggregation(Operation.SIZE, "B", "size"),
                Aggregation(Operation.NUNIQUE, "B", "nunique"),
                Aggregation(Operation.MIN, "B", "min"),
                Aggregation(Operation.MAX, "B", "max"),
                Aggregation(Operation.FIRST, "B", "first"),
            ],
        )
        assert_frame_equal(
            result,
            pd.DataFrame(
                {
                    "A": [1],
                    "size": [3],
                    "nunique": [2],
                    "min": pd.Series(["a"], dtype="category"),
                    "max": pd.Series(["b"], dtype="category"),
                    "first": pd.Series(["a"], dtype="category"),
                }
            ),
        )

    def test_aggregate_text_category_values_max(self):
        # https://github.com/pandas-dev/pandas/issues/28641
        result = groupby(
            pd.DataFrame(
                {"A": [1997], "B": pd.Series(["30-SEP-97"], dtype="category")}
            ),
            [Group("A", None)],
            [Aggregation(Operation.MAX, "B", "X")],
        )
        assert_frame_equal(
            result,
            pd.DataFrame(
                {"A": [1997], "X": pd.Series(["30-SEP-97"], dtype="category")}
            ),
        )

    def test_aggregate_text_category_values_with_multiple_agg_columns(self):
        # Typo uncovered [2019-11-14] meant we'd only call .as_ordered() when
        # the _last_ column was categorical (as opposed to when the
        # _categorical_ column was categorical).
        result = groupby(
            pd.DataFrame({"A": [1997], "B": pd.Series(["b"], dtype="category")}),
            [Group("A", None)],
            [
                Aggregation(Operation.MAX, "B", "X"),
                Aggregation(Operation.MIN, "A", "Y"),
            ],
        )
        assert_frame_equal(
            result,
            pd.DataFrame(
                {"A": [1997], "X": pd.Series(["b"], dtype="category"), "Y": [1997]}
            ),
        )

    def test_first_in_category(self):
        result = groupby(
            pd.DataFrame(
                {"A": ["MANHATTAN", "BROOKLYN"], "B": ["2018-02-28", "2018-02-28"]},
                dtype="category",
            ),
            [Group("B", None)],
            [Aggregation(Operation.NUNIQUE, "A", "first")],
        )
        assert_frame_equal(
            result,
            pd.DataFrame(
                {
                    "B": ["2018-02-28"],
                    "first": ["MANHATTAN"],
                },
                dtype="category",
            ),
        )

    def test_aggregate_text_category_values_empty_still_has_object_dtype(self):
        result = groupby(
            pd.DataFrame({"A": [None]}, dtype=str).astype("category"),
            [Group("A", None)],
            [
                Aggregation(Operation.SIZE, "A", "size"),
                Aggregation(Operation.NUNIQUE, "A", "nunique"),
                Aggregation(Operation.MIN, "A", "min"),
                Aggregation(Operation.MAX, "A", "max"),
                Aggregation(Operation.FIRST, "A", "first"),
            ],
        )
        assert_frame_equal(
            result,
            pd.DataFrame(
                {
                    "A": pd.Series([], dtype=str).astype("category"),
                    "size": pd.Series([], dtype=int),
                    "nunique": pd.Series([], dtype=int),
                    "min": pd.Series([], dtype=str).astype("category"),
                    "max": pd.Series([], dtype=str).astype("category"),
                    "first": pd.Series([], dtype=str).astype("category"),
                }
            ),
        )

    def test_aggregate_timestamp_no_granularity(self):
        result = groupby(
            pd.DataFrame({"A": [dt(2018, 1, 4), dt(2018, 1, 5), dt(2018, 1, 4)]}),
            [Group("A", None)],
            [Aggregation(Operation.SIZE, "", "size")],
        )
        assert_frame_equal(
            result,
            pd.DataFrame({"A": [dt(2018, 1, 4), dt(2018, 1, 5)], "size": [2, 1]}),
        )

    def test_aggregate_timestamp_by_second(self):
        result = groupby(
            pd.DataFrame(
                {
                    "A": [
                        dt(2018, 1, 4, 1, 2, 3, 200),
                        dt(2018, 1, 4, 1, 2, 7, 432),
                        dt(2018, 1, 4, 1, 2, 3, 123),
                    ]
                }
            ),
            [Group("A", DateGranularity.SECOND)],
            [Aggregation(Operation.SIZE, "", "size")],
        )
        assert_frame_equal(
            result,
            pd.DataFrame(
                {
                    "A": [dt(2018, 1, 4, 1, 2, 3), dt(2018, 1, 4, 1, 2, 7)],
                    "size": [2, 1],
                }
            ),
        )

    def test_aggregate_timestamp_by_minute(self):
        result = groupby(
            pd.DataFrame(
                {
                    "A": [
                        dt(2018, 1, 4, 1, 2, 3),
                        dt(2018, 1, 4, 1, 8, 3),
                        dt(2018, 1, 4, 1, 2, 8),
                    ]
                }
            ),
            [Group("A", DateGranularity.MINUTE)],
            [Aggregation(Operation.SIZE, "", "size")],
        )
        assert_frame_equal(
            result,
            pd.DataFrame(
                {"A": [dt(2018, 1, 4, 1, 2), dt(2018, 1, 4, 1, 8)], "size": [2, 1]}
            ),
        )

    def test_aggregate_timestamp_by_hour(self):
        result = groupby(
            pd.DataFrame(
                {
                    "A": [
                        dt(2018, 1, 4, 1, 2, 3),
                        dt(2018, 1, 4, 2, 2, 3),
                        dt(2018, 1, 4, 1, 8, 3),
                    ]
                }
            ),
            [Group("A", DateGranularity.HOUR)],
            [Aggregation(Operation.SIZE, "", "size")],
        )
        assert_frame_equal(
            result,
            pd.DataFrame({"A": [dt(2018, 1, 4, 1), dt(2018, 1, 4, 2)], "size": [2, 1]}),
        )

    def test_aggregate_timestamp_by_day(self):
        result = groupby(
            pd.DataFrame(
                {
                    "A": [
                        dt(2018, 1, 4, 1, 2, 3),
                        dt(2018, 2, 4, 1, 2, 3),
                        dt(2018, 1, 4, 1, 2, 3),
                    ]
                }
            ),
            [Group("A", DateGranularity.DAY)],
            [Aggregation(Operation.SIZE, "", "size")],
        )
        assert_frame_equal(
            result,
            pd.DataFrame({"A": [dt(2018, 1, 4), dt(2018, 2, 4)], "size": [2, 1]}),
        )

    def test_aggregate_timestamp_by_week(self):
        result = groupby(
            pd.DataFrame({"A": [dt(2018, 1, 4), dt(2018, 2, 4), dt(2018, 1, 6)]}),
            [Group("A", DateGranularity.WEEK)],
            [Aggregation(Operation.SIZE, "", "size")],
        )
        assert_frame_equal(
            result,
            pd.DataFrame({"A": [dt(2018, 1, 1), dt(2018, 1, 29)], "size": [2, 1]}),
        )

    def test_aggregate_null_timestamp_by_week(self):
        result = groupby(
            pd.DataFrame({"A": [pd.NaT]}),
            [Group("A", DateGranularity.WEEK)],
            [Aggregation(Operation.SIZE, "", "size")],
        )
        self.assertEqual(len(result), 0)

    def test_aggregate_timestamp_by_month(self):
        result = groupby(
            pd.DataFrame({"A": [dt(2018, 1, 4), dt(2018, 2, 4), dt(2018, 1, 6)]}),
            [Group("A", DateGranularity.MONTH)],
            [Aggregation(Operation.SIZE, "", "size")],
        )
        assert_frame_equal(
            result,
            pd.DataFrame({"A": [dt(2018, 1, 1), dt(2018, 2, 1)], "size": [2, 1]}),
        )

    def test_aggregate_timestamp_by_quarter(self):
        result = groupby(
            pd.DataFrame({"A": [dt(2018, 1, 4), dt(2018, 6, 4), dt(2018, 3, 4)]}),
            [Group("A", DateGranularity.QUARTER)],
            [Aggregation(Operation.SIZE, "", "size")],
        )
        assert_frame_equal(
            result,
            pd.DataFrame({"A": [dt(2018, 1, 1), dt(2018, 4, 1)], "size": [2, 1]}),
        )

    def test_aggregate_null_timestamp_by_quarter(self):
        result = groupby(
            pd.DataFrame({"A": [pd.NaT]}),
            [Group("A", DateGranularity.QUARTER)],
            [Aggregation(Operation.SIZE, "", "size")],
        )
        self.assertEqual(len(result), 0)

    def test_aggregate_timestamp_by_year(self):
        result = groupby(
            pd.DataFrame({"A": [dt(2018, 1, 4), dt(2019, 2, 4), dt(2018, 3, 4)]}),
            [Group("A", DateGranularity.YEAR)],
            [Aggregation(Operation.SIZE, "", "size")],
        )
        assert_frame_equal(
            result,
            pd.DataFrame({"A": [dt(2018, 1, 1), dt(2019, 1, 1)], "size": [2, 1]}),
        )


class RenderTest(unittest.TestCase):
    # def test_defaults_count(self):
    #    table = pd.DataFrame({'A': [1, 2]})
    #    result = render(table, {
    #        'groups': {
    #            'colnames': '',
    #            'group_dates': False,
    #            'date_granularities': {},
    #        },
    #        'aggregations': [],
    #    })
    #    assert_frame_equal(result, pd.DataFrame({'Group Size': [2]}))

    def test_count_no_colnames_is_no_op_TEMPORARY(self):
        # Added for https://www.pivotaltracker.com/story/show/164375369
        # Change behavior for https://www.pivotaltracker.com/story/show/164375318
        table = pd.DataFrame({"A": [1, 2]})
        result = render(
            table,
            {
                "groups": {
                    "colnames": [],
                    "group_dates": False,
                    "date_granularities": {},
                },
                "aggregations": [],
            },
        )
        assert_frame_equal(result, pd.DataFrame({"A": [1, 2]}))

    def test_count_with_colnames(self):
        # Check for obvious bug when adding
        # https://www.pivotaltracker.com/story/show/164375369
        table = pd.DataFrame({"A": [1, 2]})
        result = render(
            table,
            {
                "groups": {
                    "colnames": ["A"],
                    "group_dates": False,
                    "date_granularities": {},
                },
                "aggregations": [],
            },
        )
        assert_frame_equal(result, pd.DataFrame({"A": [1, 2], "Group Size": [1, 1]}))

    def test_quickfix_convert_value_strings_to_numbers(self):
        result = render(
            pd.DataFrame({"A": [1, 1, 1], "B": ["a", "b", "a"], "C": ["a", "b", "a"]}),
            {
                "groups": {
                    "colnames": ["A"],
                    "group_dates": False,
                    "date_granularities": {},
                },
                "aggregations": [
                    {"operation": "mean", "colname": "B", "outname": "mean"},
                    {"operation": "sum", "colname": "C", "outname": "sum"},
                ],
            },
        )
        self.assertEqual(
            result,
            {
                "error": i18n_message(
                    "non_numeric_colnames.error", {"n_columns": 2, "first_colname": "B"}
                ),
                "quick_fixes": [
                    {
                        "text": i18n_message("non_numeric_colnames.quick_fix.text"),
                        "action": "prependModule",
                        "args": ["converttexttonumber", {"colnames": ["B", "C"]}],
                    }
                ],
            },
        )

    def test_ignore_non_date_datetimes(self):
        # Steps for the user to get here:
        # 1. Make a date column, 'A'
        # 2. Check "Group Dates". The column appears.
        # 3. Select column 'A', and select a date granularity for it
        # 4. Alter the input DataFrame such that 'A' is no longer datetime
        #
        # Expected results: you can't group it by date any more.
        result = render(
            pd.DataFrame(
                {
                    "A": [1],  # "used to be a datetime" according to above steps
                    "B": [dt(2019, 1, 4)],  # so we don't trigger quickfix
                }
            ),
            {
                "groups": {
                    "colnames": ["A"],
                    "group_dates": True,
                    "date_granularities": {"A": "T"},
                },
                "aggregations": [
                    {"operation": "size", "colname": "", "outname": "size"}
                ],
            },
        )
        assert_frame_equal(result, pd.DataFrame({"A": [1], "size": [1]}))


if __name__ == "__main__":
    unittest.main()
