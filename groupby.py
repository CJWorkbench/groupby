from collections import namedtuple
from enum import Enum
from typing import Dict, List, Optional, Union, Set
import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype


def migrate_params(params):
    # v2 looks like:
    # "groups": {
    #   "type": "dict",
    #   "properties": {
    #     "colnames": { "type": "multicolumn" },
    #     "date_granularities": { "type": "dict" }
    #   }
    # },
    # "aggregations": {
    #   "type": "list",
    #   "inner_dtype": {
    #     "type": "dict",
    #     "properties": {
    #       "operation": { "type": "string" },
    #       "colname": { "type": "column" },
    #       "outname": { "type": "string" }
    #     }
    #   }
    # }
    if not ('groups' in params and 'aggregations' in params):
        params = _migrate_params_v1_to_v2(params)

    return params


def _migrate_params_v1_to_v2(params):
    # v1 looked like:
    # active.addremove.last|groupby|1
    # active.addremove|operation|1
    # active.addremove|operation|2
    # active.addremove|operation|3
    # active.addremove|operation|4
    # cheat.cheat|operation|1
    # cheat.cheat|operation|2
    # cheat.cheat|operation|3
    # groupby|groupby|0
    # groupby|groupby|1
    # operation|operation|0
    # operation.show-sibling|operation|1
    # operation.show-sibling|operation|2
    # operation.show-sibling|operation|3
    # operation.show-sibling|operation|4
    # outputname|operation|0
    # outputname|operation|1
    # outputname|operation|2
    # outputname|operation|3
    # outputname|operation|4
    # targetcolumn.hide-with-sibling|operation|1
    # targetcolumn.hide-with-sibling|operation|2
    # targetcolumn.hide-with-sibling|operation|3
    # targetcolumn.hide-with-sibling|operation|4
    # targetcolumn|operation|0
    groupby = [
        params.get('groupby|groupby|0', ''),
        (
            params.get('groupby|groupby|1', '')
            if params.get('active.addremove.last|groupby|1') else ''
        )
    ]
    groupby = filter(lambda x: not not x, groupby)

    groups = {
        'colnames': ','.join(groupby),
        'group_dates': False,
        'date_granularities': {},
    }

    aggregations = []
    for active_key, operation_key, colname_key, outname_key in [
        # Generated by copy/pasting from the old spec.
        (
            None,
            'operation|operation|0',
            'targetcolumn|operation|0',
            'outputname|operation|0'
        ),
        (
            'active.addremove|operation|1',
            'operation.show-sibling|operation|1',
            'targetcolumn.hide-with-sibling|operation|1',
            'outputname|operation|1'
        ),
        (
            'active.addremove|operation|2',
            'operation.show-sibling|operation|2',
            'targetcolumn.hide-with-sibling|operation|2',
            'outputname|operation|2'
        ),
        (
            'active.addremove|operation|3',
            'operation.show-sibling|operation|3',
            'targetcolumn.hide-with-sibling|operation|3',
            'outputname|operation|3'
        ),
        (
            'active.addremove|operation|4',
            'operation.show-sibling|operation|4',
            'targetcolumn.hide-with-sibling|operation|4',
            'outputname|operation|4'
        ),
    ]:
        if active_key is not None and not params.get(active_key, False):
            # If active checkbox is unchecked, stop all conversions (only
            # applies after first conversion, which has active_key = None)
            break

        try:
            operation_number = params.get(operation_key)  # or None
            operation = ['size', 'nunique', 'sum', 'mean', 'min',
                         'max'][operation_number]
        except (TypeError, IndexError):
            # Fold away non-sane aggregations
            continue

        colname = params.get(colname_key, '')
        outname = params.get(outname_key, '')

        if operation in {'size', 'nunique'}:
            colname = ''
        else:
            if not colname:
                # Non-sane aggregation: nix it
                continue

        aggregations.append({
            'operation': operation,
            'colname': colname,
            'outname': outname,
        })

    return {
        'groups': groups,
        'aggregations': aggregations,
    }


class DateGranularity(Enum):
    # Frequencies are as in pandas. See
    # http://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases
    SECOND = 'S'
    MINUTE = 'T'
    HOUR = 'H'
    DAY = 'D'
    MONTH = 'M'
    QUARTER = 'Q'
    YEAR = 'Y'

    @property
    def numpy_unit(self):
        return {
            self.SECOND: 's',
            self.MINUTE: 'm',
            self.HOUR: 'h',
            self.DAY: 'D',
            self.MONTH: 'M',
            self.YEAR: 'Y',
        }[self]


class Operation(Enum):
    # Aggregate function names as in pandas. See
    # https://pandas.pydata.org/pandas-docs/stable/api.html#computations-descriptive-stats
    SIZE = 'size'
    NUNIQUE = 'nunique'
    SUM = 'sum'
    MEAN = 'mean'
    MIN = 'min'
    MAX = 'max'
    FIRST = 'first'

    def needs_column(self):
        return self != self.SIZE

    def needs_numeric_column(self):
        return self in {
            self.SUM,
            self.MEAN,
        }

    def default_outname(self, colname):
        if self == self.SIZE:
            return 'Group Size'

        verb = {
            self.NUNIQUE: 'Unique count',
            self.SUM: 'Sum',
            self.MEAN: 'Average',
            self.MIN: 'Minimum',
            self.MAX: 'Maximum',
            self.FIRST: 'First',
        }[self]

        return '%s of %s' % (verb, colname)


Group = namedtuple('Group', ['colname', 'date_granularity'])
Aggregation = namedtuple('Aggregation', ['operation', 'colname', 'outname'])


def parse_groups(*, date_colnames: Set[str], colnames: str, group_dates: bool,
                 date_granularities: Dict[str, str]) -> List[Group]:
    colnames = [c for c in colnames.split(',') if c]
    groups = []
    for colname in colnames:
        granularity_str = date_granularities.get(colname, '')
        if group_dates and colname in date_colnames and granularity_str:
            granularity = DateGranularity(granularity_str)
        else:
            granularity = None
        groups.append(Group(colname, granularity))
    return groups


def parse_aggregation(*, operation: str, colname: str,
                      outname: str) -> Optional[Aggregation]:
    operation = Operation(operation)
    if not colname and operation != Operation.SIZE:
        # Workbench clears empty colnames. Nix the entire Aggregation.
        return None
    if not outname:
        outname = operation.default_outname(colname)
    return Aggregation(operation, colname, outname)


def parse_aggregations(aggregations: List[Dict[str, str]]
                       ) -> List[Aggregation]:
    aggregations = [parse_aggregation(**kwargs) for kwargs in aggregations]
    return [a for a in aggregations if a is not None]


def group_to_spec(group: Group, table: pd.DataFrame) -> Union[str, pd.Series]:
    """
    Convert a Group to a Pandas .groupby() list item.
    """
    if group.date_granularity is None:
        return group.colname
    else:
        series = table[group.colname]
        if group.date_granularity == DateGranularity.QUARTER:
            # numpy has no "quarter" so we'll need to do something funky
            month_numbers = series.values.astype('M8[M]').astype('int')
            rounded_month_numbers = (np.floor_divide(month_numbers, 3) * 3)
            values = rounded_month_numbers.astype('M8[M]')
        else:
            freq = group.date_granularity.numpy_unit
            values = series.values.astype('M8[' + freq + ']')
        return pd.Series(values, name=group.colname)


def groupby(table: pd.DataFrame, groups: List[Group],
            aggregations: List[Aggregation]) -> pd.DataFrame:
    group_specs = [group_to_spec(group, table) for group in groups]

    # Build agg_sets: {colname => {op1, op2}}
    #
    # We'll pass this to pandas. Don't worry about ordering.
    agg_sets = {}
    for aggregation in aggregations:
        if aggregation.operation != Operation.SIZE:
            op = aggregation.operation.value
            colname = aggregation.colname
            agg_sets.setdefault(colname, set()).add(op)
    if not agg_sets:
        # We need to pass _something_ to agg(). Pass 'size', which is
        # (hopefully) the least computationally-intense.
        agg_sets = 'size'

    if group_specs:
        # aggs: DataFrame indexed by group
        # out: just the group colnames, no values yet (we'll add them later)
        grouped = table.groupby(group_specs)
        if agg_sets:
            aggs = grouped.agg(agg_sets)
        out = aggs.index.to_frame(index=False)
    else:
        # aggs: DataFrame with just one row
        # out: one empty row, no columns yet
        grouped = table
        if agg_sets:
            aggs = table.agg(agg_sets)
        out = pd.DataFrame(columns=[], index=[0])

    # Now copy values from `aggs` into `out`. (They have the same index.)
    for aggregation in aggregations:
        op = aggregation.operation.value
        outname = aggregation.outname
        colname = aggregation.colname

        if not outname:
            outname = aggregation.operation.default_outname(
                aggregation.colname
            )

        if aggregation.operation == Operation.SIZE:
            if group_specs:
                out[outname] = grouped.size().values
            else:
                out[outname] = len(table)
        else:
            series = aggs[colname][op]
            # Depending on op, pandas may return a series-like or an array.
            try:
                out[outname] = series.values
            except AttributeError:
                out[outname] = series

    return out


def render(table, params):
    colnames = table.columns
    date_colnames = set(colname for colname in colnames
                        if hasattr(table[colname], 'dt'))
    groups = parse_groups(date_colnames=date_colnames, **params['groups'])
    aggregations = parse_aggregations(params['aggregations'])

    # HACK: the default the user sees is "Count" because our onboarding path
    # is for the user to 1. Select a Group By column and 2. view the count.
    #
    # _However_, this is a "Group By" module so we need to support the obvious
    # operation, 'SELECT COUNT(*) FROM input'. The obvious way to display that
    # is to select "Count" and not select a Group By column.
    #
    # ... and unfortunately, that form setup -- no columns selected, one
    # "Count" aggregation selected -- is exactly what the user sees by default
    # after adding the module, before step 1 of the onboarding path.
    #
    # So we get a tough choice: either make "no aggregations" a no-op to give
    # us the ideal onboarding path, _OR_ make "no aggregations" default to
    # "count", to support the obvious operation. Pick one: complete+simple, or
    # onboarding-friendly. Workbench can't give both, the way its forms are
    # designed right now.
    if not aggregations:
        # We've chosen "complete+simple" over "onboarding-friendly"
        aggregations.append(Aggregation(Operation.SIZE, '', ''))

    # Error out with a quickfix if aggregations need int and we're not int
    non_numeric_colnames = []
    for aggregation in aggregations:
        if aggregation.operation.needs_numeric_column():
            colname = aggregation.colname
            series = table[colname]
            if (
                not is_numeric_dtype(series)
                and colname not in non_numeric_colnames
            ):
                non_numeric_colnames.append(colname)
    if non_numeric_colnames:
        if len(non_numeric_colnames) == 1:
            pluralized_column = 'Column'
        else:
            pluralized_column = 'Columns'

        return {
            'error': (
                '%s %s must be Numbers' % (
                    pluralized_column,
                    ', '.join(f'"{x}"' for x in non_numeric_colnames)
                )
            ),
            'quick_fixes': [
                {
                    'text': 'Convert',
                    'action': 'prependModule',
                    'args': [
                        'extractnumbers',
                        {'colnames': ','.join(non_numeric_colnames)},
                    ],
                },
            ],
        }

    return groupby(table, groups, aggregations)
