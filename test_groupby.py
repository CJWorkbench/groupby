from datetime import datetime as dt
import unittest
import pandas as pd
from pandas.testing import assert_frame_equal
from groupby import render, migrate_params, groupby, Group, Aggregation, \
        Operation, DateGranularity


class MigrateParamsV1Test(unittest.TestCase):
    defaults = {
      'groupby|groupby|0': '',
      'groupby|groupby|1': '',
      'active.addremove.last|groupby|1': False,
      'operation|operation|0': 0,
      'targetcolumn|operation|0': '',
      'outputname|operation|0': '',
      'operation.show-sibling|operation|1': 0,
      'targetcolumn.hide-with-sibling|operation|1': '',
      'outputname|operation|1': '',
      'cheat.cheat|operation|1': False,
      'active.addremove|operation|1': False,
      'operation.show-sibling|operation|2': 0,
      'targetcolumn.hide-with-sibling|operation|2': '',
      'outputname|operation|2': '',
      'cheat.cheat|operation|2': False,
      'active.addremove|operation|2': False,
      'operation.show-sibling|operation|3': 0,
      'targetcolumn.hide-with-sibling|operation|3': '',
      'outputname|operation|3': '',
      'cheat.cheat|operation|3': False,
      'active.addremove|operation|3': False,
      'operation.show-sibling|operation|4': 0,
      'targetcolumn.hide-with-sibling|operation|4': '',
      'outputname|operation|4': '',
      'cheat.cheat|operation|4': False,
      'active.addremove.last|operation|4': False
    }

    def test_migrate_default(self):
        self.assertEqual(migrate_params(self.defaults), {
            'groups': {
                'colnames': '',
                'group_dates': False,
                'date_granularities': {},
            },
            'aggregations': [
                {
                    'operation': 'size',
                    'colname': '',
                    'outname': '',
                },
            ],
        })

    def test_migrate_two_colnames(self):
        self.assertEqual(migrate_params({
            **self.defaults,
            'groupby|groupby|0': 'a',
            'groupby|groupby|1': 'b',
            'active.addremove.last|groupby|1': True,
        })['groups']['colnames'], 'a,b')

    def test_migrate_two_colnames_but_second_not_active(self):
        self.assertEqual(migrate_params({
            **self.defaults,
            'groupby|groupby|0': 'a',
            'groupby|groupby|1': 'b',
            'active.addremove.last|groupby|1': False,
        })['groups']['colnames'], 'a')

    def test_migrate_aggregation(self):
        self.assertEqual(migrate_params({
            **self.defaults,
            'operation|operation|0': 5,
            'targetcolumn|operation|0': 'c',
            'outputname|operation|0': 'C',
        })['aggregations'], [
            {'operation': 'max', 'colname': 'c', 'outname': 'C'},
        ])

    def test_migrate_only_active_aggregations(self):
        self.assertEqual(migrate_params({
            **self.defaults,
            'operation|operation|0': 5,
            'targetcolumn|operation|0': 'c',
            'outputname|operation|0': 'C',
            'active.addremove|operation|1': False,
            # The next operation isn't active, so it will be ignored
            'operation.show-sibling|operation|1': 2,
            'targetcolumn.hide-with-sibling|operation|1': 'd',
            'outputname|operation|1': 'D',
        })['aggregations'], [
            {'operation': 'max', 'colname': 'c', 'outname': 'C'},
        ])

    def test_migrate_many_aggregations(self):
        self.assertEqual(migrate_params({
            **self.defaults,
            # COUNT(*) AS A
            'operation|operation|0': 0,
            'targetcolumn|operation|0': '',
            'outputname|operation|0': 'A',
            # COUNT DISTINCT(*) AS B
            'active.addremove|operation|1': True,
            'operation.show-sibling|operation|1': 1,
            'targetcolumn.hide-with-sibling|operation|1': '',
            'outputname|operation|1': 'B',
            # SUM(c) AS C
            'active.addremove|operation|2': True,
            'operation.show-sibling|operation|2': 2,
            'targetcolumn.hide-with-sibling|operation|2': 'c',
            'outputname|operation|2': 'C',
            # MEAN(d) AS D
            'active.addremove|operation|3': True,
            'operation.show-sibling|operation|3': 3,
            'targetcolumn.hide-with-sibling|operation|3': 'd',
            'outputname|operation|3': 'D',
            # MAX(e) AS E
            'active.addremove|operation|4': True,
            'operation.show-sibling|operation|4': 4,
            'targetcolumn.hide-with-sibling|operation|4': 'e',
            'outputname|operation|4': 'E',
        })['aggregations'], [
            {'operation': 'size', 'colname': '', 'outname': 'A'},
            {'operation': 'nunique', 'colname': '', 'outname': 'B'},
            {'operation': 'sum', 'colname': 'c', 'outname': 'C'},
            {'operation': 'mean', 'colname': 'd', 'outname': 'D'},
            {'operation': 'min', 'colname': 'e', 'outname': 'E'},
        ])

    def test_migrate_omit_aggregation_missing_colname(self):
        self.assertEqual(migrate_params({
            **self.defaults,
            # SUM(*) AS A (which isn't valid)
            'operation|operation|0': 2,
            'targetcolumn|operation|0': '',
            'outputname|operation|0': 'A',
            # COUNT DISTINCT(*) AS B
            'active.addremove|operation|1': True,
            'operation.show-sibling|operation|1': 1,
            'targetcolumn.hide-with-sibling|operation|1': '',
            'outputname|operation|1': 'B',
        })['aggregations'], [
            {'operation': 'nunique', 'colname': '', 'outname': 'B'},
        ])


class GroupbyTest(unittest.TestCase):
    def test_no_colnames(self):
        table = pd.DataFrame({'A': [1, 2]})
        result = groupby(table, [], [Aggregation(Operation.SUM, 'A', 'X')])
        assert_frame_equal(result, pd.DataFrame({'X': [3]}))

    def test_size(self):
        table = pd.DataFrame({'A': [1, 1, 2]})
        result = groupby(table, [Group('A', None)],
                         [Aggregation(Operation.SIZE, '', 'X')])
        assert_frame_equal(result, pd.DataFrame({'A': [1, 2], 'X': [2, 1]}))

    def test_multilevel(self):
        result = groupby(
            pd.DataFrame({
                'A': [1, 1, 1, 2],
                'B': [1, 1, 2, 2],
                'C': [0, 1, -1, 0],
            }),
            [Group('A', None), Group('B', None)],
            [Aggregation(Operation.SUM, 'C', 'D')]
        )
        assert_frame_equal(result, pd.DataFrame({
            'A': [1, 1, 2],
            'B': [1, 2, 2],
            'D': [1, -1, 0],
        }))

    def test_allow_duplicate_aggregations(self):
        result = groupby(
            pd.DataFrame({'A': [1, 1, 2], 'B': [1, 2, 3]}),
            [Group('A', None)],
            [
                Aggregation(Operation.MIN, 'B', 'X'),
                Aggregation(Operation.MIN, 'B', 'Y'),
            ]
        )
        assert_frame_equal(result, pd.DataFrame({
            'A': [1, 2],
            'X': [1, 3],
            'Y': [1, 3],
        }))

    def test_aggregate_grouped_column(self):
        # Does the user know what he or she is doing? Dunno. But
        # [adamhooper, 2019-01-03] SQL would aggregate the single
        # value, so why shouldn't Workbench?
        result = groupby(
            pd.DataFrame({
                'A': [1, 1, 2],
                'B': [1, 2, 2],
                'C': [5, 5, 5],
            }),
            [Group('A', None), Group('B', None)],
            [Aggregation(Operation.SUM, 'B', 'X')]
        )
        assert_frame_equal(result, pd.DataFrame({
            'A': [1, 1, 2],
            'B': [1, 2, 2],
            'X': [1, 2, 2],  # just a copy of B
        }))

    def test_aggregate_numbers(self):
        result = groupby(
            pd.DataFrame({
                'A': [2, 1, 2, 2],
                'B': [1, 2, 5, 1],
            }),
            [Group('A', None)],
            [
                Aggregation(Operation.SIZE, '', 'size'),
                Aggregation(Operation.NUNIQUE, 'B', 'nunique'),
                Aggregation(Operation.SUM, 'B', 'sum'),
                Aggregation(Operation.MEAN, 'B', 'mean'),
                Aggregation(Operation.MIN, 'B', 'min'),
                Aggregation(Operation.MAX, 'B', 'max'),
                Aggregation(Operation.FIRST, 'B', 'first'),
            ]
        )
        assert_frame_equal(result, pd.DataFrame({
            'A': [1, 2],
            'size': [1, 3],
            'nunique': [1, 2],
            'sum': [2, 7],
            'mean': [2, 7 / 3],
            'min': [2, 1],
            'max': [2, 5],
            'first': [2, 1],
        }))

    def test_aggregate_strings(self):
        result = groupby(
            pd.DataFrame({
                'A': [1, 1, 1],
                'B': ['a', 'b', 'a'],
            }),
            [Group('A', None)],
            [
                Aggregation(Operation.SIZE, 'B', 'size'),
                Aggregation(Operation.NUNIQUE, 'B', 'nunique'),
                Aggregation(Operation.MIN, 'B', 'min'),
                Aggregation(Operation.MAX, 'B', 'max'),
                Aggregation(Operation.FIRST, 'B', 'first'),
            ]
        )
        assert_frame_equal(result, pd.DataFrame({
            'A': [1],
            'size': [3],
            'nunique': [2],
            'min': ['a'],
            'max': ['b'],
            'first': ['a'],
        }))

    def test_aggregate_datetime_no_granularity(self):
        result = groupby(
            pd.DataFrame({
                'A': [dt(2018, 1, 4), dt(2018, 1, 5), dt(2018, 1, 4)],
            }),
            [Group('A', None)],
            [Aggregation(Operation.SIZE, '', 'size')]
        )
        assert_frame_equal(result, pd.DataFrame({
            'A': [dt(2018, 1, 4), dt(2018, 1, 5)],
            'size': [2, 1],
        }))

    def test_aggregate_datetime_by_second(self):
        result = groupby(
            pd.DataFrame({
                'A': [
                    dt(2018, 1, 4, 1, 2, 3, 200),
                    dt(2018, 1, 4, 1, 2, 7, 432),
                    dt(2018, 1, 4, 1, 2, 3, 123),
                ],
            }),
            [Group('A', DateGranularity.SECOND)],
            [Aggregation(Operation.SIZE, '', 'size')]
        )
        assert_frame_equal(result, pd.DataFrame({
            'A': [dt(2018, 1, 4, 1, 2, 3), dt(2018, 1, 4, 1, 2, 7)],
            'size': [2, 1],
        }))

    def test_aggregate_datetime_by_minute(self):
        result = groupby(
            pd.DataFrame({
                'A': [
                    dt(2018, 1, 4, 1, 2, 3),
                    dt(2018, 1, 4, 1, 8, 3),
                    dt(2018, 1, 4, 1, 2, 8),
                ],
            }),
            [Group('A', DateGranularity.MINUTE)],
            [Aggregation(Operation.SIZE, '', 'size')]
        )
        assert_frame_equal(result, pd.DataFrame({
            'A': [dt(2018, 1, 4, 1, 2), dt(2018, 1, 4, 1, 8)],
            'size': [2, 1],
        }))

    def test_aggregate_datetime_by_hour(self):
        result = groupby(
            pd.DataFrame({
                'A': [
                    dt(2018, 1, 4, 1, 2, 3),
                    dt(2018, 1, 4, 2, 2, 3),
                    dt(2018, 1, 4, 1, 8, 3),
                ],
            }),
            [Group('A', DateGranularity.HOUR)],
            [Aggregation(Operation.SIZE, '', 'size')]
        )
        assert_frame_equal(result, pd.DataFrame({
            'A': [dt(2018, 1, 4, 1), dt(2018, 1, 4, 2)],
            'size': [2, 1],
        }))

    def test_aggregate_datetime_by_day(self):
        result = groupby(
            pd.DataFrame({
                'A': [
                    dt(2018, 1, 4, 1, 2, 3),
                    dt(2018, 2, 4, 1, 2, 3),
                    dt(2018, 1, 4, 1, 2, 3),
                ],
            }),
            [Group('A', DateGranularity.DAY)],
            [Aggregation(Operation.SIZE, '', 'size')]
        )
        assert_frame_equal(result, pd.DataFrame({
            'A': [dt(2018, 1, 4), dt(2018, 2, 4)],
            'size': [2, 1],
        }))

    def test_aggregate_datetime_by_month(self):
        result = groupby(
            pd.DataFrame({
                'A': [dt(2018, 1, 4), dt(2018, 2, 4), dt(2018, 1, 6)],
            }),
            [Group('A', DateGranularity.MONTH)],
            [Aggregation(Operation.SIZE, '', 'size')]
        )
        assert_frame_equal(result, pd.DataFrame({
            'A': [dt(2018, 1, 1), dt(2018, 2, 1)],
            'size': [2, 1],
        }))

    def test_aggregate_datetime_by_quarter(self):
        result = groupby(
            pd.DataFrame({
                'A': [dt(2018, 1, 4), dt(2018, 6, 4), dt(2018, 3, 4)],
            }),
            [Group('A', DateGranularity.QUARTER)],
            [Aggregation(Operation.SIZE, '', 'size')]
        )
        assert_frame_equal(result, pd.DataFrame({
            'A': [dt(2018, 1, 1), dt(2018, 4, 1)],
            'size': [2, 1],
        }))

    def test_aggregate_datetime_by_year(self):
        result = groupby(
            pd.DataFrame({
                'A': [dt(2018, 1, 4), dt(2019, 2, 4), dt(2018, 3, 4)],
            }),
            [Group('A', DateGranularity.YEAR)],
            [Aggregation(Operation.SIZE, '', 'size')]
        )
        assert_frame_equal(result, pd.DataFrame({
            'A': [dt(2018, 1, 1), dt(2019, 1, 1)],
            'size': [2, 1],
        }))


class RenderTest(unittest.TestCase):
    def test_defaults_no_op(self):
        table = pd.DataFrame({'A': [1, 2]})
        result = render(table, {
            'groups': {
                'colnames': '',
                'group_dates': False,
                'date_granularities': {},
            },
            'aggregations': [],
        })
        assert_frame_equal(result, table)

    def test_quickfix_convert_value_strings_to_numbers(self):
        result = render(
            pd.DataFrame({
                'A': [1, 1, 1],
                'B': ['a', 'b', 'a'],
                'C': ['a', 'b', 'a'],
            }), {
                'groups': {
                    'colnames': 'A',
                    'group_dates': False,
                    'date_granularities': {},
                },
                'aggregations': [
                    {'operation': 'mean', 'colname': 'B', 'outname': 'mean'},
                    {'operation': 'sum', 'colname': 'C', 'outname': 'sum'},
                ],
            }
        )
        self.assertEqual(result, {
            'error': 'Columns "B", "C" must be Numbers',
            'quick_fixes': [
                {
                    'text': 'Convert',
                    'action': 'prependModule',
                    'args': ['extractnumbers', {'colnames': 'B,C'}],
                },
            ],
        })

    def test_ignore_non_date_datetimes(self):
        # Steps for the user to get here:
        # 1. Make a date column, 'A'
        # 2. Check "Group Dates". The column appears.
        # 3. Select column 'A', and select a date granularity for it
        # 4. Alter the input DataFrame such that 'A' is no longer datetime
        #
        # Expected results: you can't group it by date any more.
        result = render(
            pd.DataFrame({
                'A': [1],  # "used to be a datetime" according to above steps
                'B': [dt(2019, 1, 4)],  # so we don't trigger quickfix
            }), {
                'groups': {
                    'colnames': 'A',
                    'group_dates': True,
                    'date_granularities': {'A': 'T'},
                },
                'aggregations': [
                    {'operation': 'size', 'colname': '', 'outname': 'size'},
                ],
            }
        )
        assert_frame_equal(result, pd.DataFrame({'A': [1], 'size': [1]}))


if __name__ == '__main__':
    unittest.main()
