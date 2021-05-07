from enum import Enum
from typing import Any, Callable, Dict, FrozenSet, List, NamedTuple, Optional

import numpy as np
import pyarrow as pa
import pyarrow.compute
from cjwmodule import i18n
from cjwmodule.arrow.types import ArrowRenderResult
from cjwmodule.types import QuickFix, QuickFixAction, RenderError


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
    if not ("groups" in params and "aggregations" in params):
        params = _migrate_params_v1_to_v2(params)
    if isinstance(params["groups"]["colnames"], str):
        params = _migrate_params_v2_to_v3(params)

    return params


def _migrate_params_v1_to_v2(params):
    """
    v1 looked like:

        active.addremove.last|groupby|1
        active.addremove|operation|1
        active.addremove|operation|2
        active.addremove|operation|3
        active.addremove|operation|4
        cheat.cheat|operation|1
        cheat.cheat|operation|2
        cheat.cheat|operation|3
        groupby|groupby|0
        groupby|groupby|1
        operation|operation|0
        operation.show-sibling|operation|1
        operation.show-sibling|operation|2
        operation.show-sibling|operation|3
        operation.show-sibling|operation|4
        outputname|operation|0
        outputname|operation|1
        outputname|operation|2
        outputname|operation|3
        outputname|operation|4
        targetcolumn.hide-with-sibling|operation|1
        targetcolumn.hide-with-sibling|operation|2
        targetcolumn.hide-with-sibling|operation|3
        targetcolumn.hide-with-sibling|operation|4
        targetcolumn|operation|0
    """
    groupby = [
        params.get("groupby|groupby|0", ""),
        (
            params.get("groupby|groupby|1", "")
            if params.get("active.addremove.last|groupby|1")
            else ""
        ),
    ]
    groupby = filter(lambda x: not not x, groupby)

    groups = {
        "colnames": ",".join(groupby),
        "group_dates": False,
        "date_granularities": {},
    }

    aggregations = []
    for active_key, operation_key, colname_key, outname_key in [
        # Generated by copy/pasting from the old spec.
        (
            None,
            "operation|operation|0",
            "targetcolumn|operation|0",
            "outputname|operation|0",
        ),
        (
            "active.addremove|operation|1",
            "operation.show-sibling|operation|1",
            "targetcolumn.hide-with-sibling|operation|1",
            "outputname|operation|1",
        ),
        (
            "active.addremove|operation|2",
            "operation.show-sibling|operation|2",
            "targetcolumn.hide-with-sibling|operation|2",
            "outputname|operation|2",
        ),
        (
            "active.addremove|operation|3",
            "operation.show-sibling|operation|3",
            "targetcolumn.hide-with-sibling|operation|3",
            "outputname|operation|3",
        ),
        (
            "active.addremove|operation|4",
            "operation.show-sibling|operation|4",
            "targetcolumn.hide-with-sibling|operation|4",
            "outputname|operation|4",
        ),
    ]:
        if active_key is not None and not params.get(active_key, False):
            # If active checkbox is unchecked, stop all conversions (only
            # applies after first conversion, which has active_key = None)
            break

        try:
            operation_number = params.get(operation_key)  # or None
            operation = ["size", "nunique", "sum", "mean", "min", "max"][
                operation_number
            ]
        except (TypeError, IndexError):  # pragma: no cover
            # Fold away non-sane aggregations
            continue

        colname = params.get(colname_key, "")
        outname = params.get(outname_key, "")

        if operation in {"size", "nunique"}:
            colname = ""
        else:
            if not colname:
                # Non-sane aggregation: nix it
                continue

        aggregations.append(
            {"operation": operation, "colname": colname, "outname": outname}
        )

    return {"groups": groups, "aggregations": aggregations}


def _migrate_params_v2_to_v3(params):
    """
    v2: params['groups']['colnames'] is comma-separated str. v3: List[str].
    """
    return {
        "groups": {
            "colnames": [c for c in params["groups"]["colnames"].split(",") if c],
            "group_dates": params["groups"]["group_dates"],
            "date_granularities": params["groups"]["date_granularities"],
        },
        "aggregations": params["aggregations"],
    }


class DateGranularity(Enum):
    # Frequencies are as in pandas. See
    # http://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases
    SECOND = "S"
    MINUTE = "T"
    HOUR = "H"
    DAY = "D"
    WEEK = "W"
    MONTH = "M"
    QUARTER = "Q"
    YEAR = "Y"

    @property
    def numpy_unit(self):
        return {
            self.SECOND: "s",
            self.MINUTE: "m",
            self.HOUR: "h",
            self.DAY: "D",
            self.MONTH: "M",
            self.YEAR: "Y",
        }[self]

    @property
    def date_unit(self):
        return {
            self.DAY: "day",
            self.WEEK: "week",
            self.MONTH: "month",
            self.QUARTER: "quarter",
            self.YEAR: "year",
        }[self]

    @property
    def rounding_unit(self):
        return {
            self.HOUR: "hour",
            self.MINUTE: "minute",
            self.SECOND: "second",
        }[self]


def nonnull_group_splits(array: pa.Array, group_splits: np.array) -> np.array:
    # in an array [null, 1, null, 2, null]
    # with group_splits [1, 2, 3], groups are [null], [1], [null], [2, null]
    # n_nulls_by_index will be [1, 1, 2, 2, 3]
    n_nulls_by_index = np.cumsum(
        array.is_null().to_numpy(zero_copy_only=False),
        dtype=np.min_scalar_type(-len(array)),
    )
    # non-null array is [1, 2]
    # we want groups [], [1], [], [2]
    # we want nonnull_group_splits [0, 1, 1]
    return group_splits - n_nulls_by_index[group_splits - 1]


def size(*, num_rows: int, group_splits: np.array, **kwargs) -> pa.Array:
    starts = np.insert(group_splits, 0, 0)
    ends = np.append(group_splits, num_rows)
    return pa.array(ends - starts, pa.int64())


def nunique(*, array: pa.Array, group_splits: np.array, **kwargs) -> pa.Array:
    nonnull_splits = nonnull_group_splits(array, group_splits)
    nonnull_values = array.filter(array.is_valid()).to_numpy(zero_copy_only=False)
    counts = np.fromiter(
        (np.unique(subarr).size for subarr in np.split(nonnull_values, nonnull_splits)),
        dtype=np.int64,
        count=len(nonnull_splits) + 1,
    )
    return pa.array(counts)


def first(*, array: pa.Array, group_splits: np.array, **kwargs) -> pa.Array:
    nonnull_values = array.filter(array.is_valid())
    nonnull_splits = nonnull_group_splits(array, group_splits)
    starts = np.insert(nonnull_splits, 0, 0)
    ends = np.append(nonnull_splits, len(nonnull_values))
    nulls = starts == ends
    indices = pa.array(starts, pa.int64(), mask=nulls)
    return nonnull_values.take(indices)  # taking index NULL gives NULL


def build_ufunc_wrapper(
    np_func: Callable[[Any], np.array], force_otype=None
) -> Callable[..., pa.Array]:
    def call_ufunc(values: np.array, group_splits: np.array, otype, zero) -> np.array:
        starts = np.append([0], group_splits)
        ends = np.append(group_splits, len(values))
        values = [
            zero if start == end else np_func(values[start:end])
            for start, end in zip(starts, ends)
        ]
        nulls = starts == ends
        return np.array(values, dtype=otype), nulls

    def ufunc_caller(*, array: pa.Array, group_splits: np.array, **kwargs) -> pa.Array:
        nonnull_splits = nonnull_group_splits(array, group_splits)
        nonnull_values = array.filter(array.is_valid()).to_numpy(zero_copy_only=False)
        if force_otype:
            otype = force_otype
        else:
            otype = nonnull_values.dtype
        if pa.types.is_unicode(array.type):
            zero = ""
        else:
            zero = otype.type()
        np_result, np_nulls = call_ufunc(nonnull_values, nonnull_splits, otype, zero)
        return pa.array(np_result, mask=np_nulls)

    return ufunc_caller


sum = build_ufunc_wrapper(np.sum)
mean = build_ufunc_wrapper(np.mean, force_otype=np.dtype("float64"))
median = build_ufunc_wrapper(np.median, force_otype=np.dtype("float64"))
min = build_ufunc_wrapper(np.amin)
max = build_ufunc_wrapper(np.amax)


class Operation(Enum):
    # Aggregate function names as in pandas. See
    # https://pandas.pydata.org/pandas-docs/stable/api.html#computations-descriptive-stats
    SIZE = "size"
    NUNIQUE = "nunique"
    SUM = "sum"
    MEAN = "mean"
    MEDIAN = "median"
    MIN = "min"
    MAX = "max"
    FIRST = "first"

    def needs_numeric_column(self):
        return self in {self.SUM, self.MEAN, self.MEDIAN}

    def default_outname(self, colname):
        if self == self.SIZE:
            return "Group Size"

        verb = {
            self.NUNIQUE: "Unique count",
            self.SUM: "Sum",
            self.MEAN: "Average",
            self.MEDIAN: "Median",
            self.MIN: "Minimum",
            self.MAX: "Maximum",
            self.FIRST: "First",
        }[self]

        return "%s of %s" % (verb, colname)


class Group(NamedTuple):
    colname: str
    date_granularity: Optional[DateGranularity]


class Aggregation(NamedTuple):
    operation: Operation
    colname: str
    outname: str


def parse_groups(
    *,
    date_colnames: FrozenSet[str],
    colnames: List[str],
    group_dates: bool,
    date_granularities: Dict[str, str],
) -> List[Group]:
    groups = []
    for colname in colnames:
        granularity_str = date_granularities.get(colname, "")
        if group_dates and colname in date_colnames and granularity_str:
            granularity = DateGranularity(granularity_str)
        else:
            granularity = None
        groups.append(Group(colname, granularity))
    return groups


def parse_aggregation(
    *, operation: str, colname: str, outname: str
) -> Optional[Aggregation]:
    operation = Operation(operation)
    if not colname and operation != Operation.SIZE:
        # Workbench clears empty colnames. Nix the entire Aggregation.
        return None
    if not outname:
        outname = operation.default_outname(colname)
    return Aggregation(operation, colname, outname)


def parse_aggregations(aggregations: List[Dict[str, str]]) -> List[Aggregation]:
    aggregations = [parse_aggregation(**kwargs) for kwargs in aggregations]
    return [a for a in aggregations if a is not None]


def make_groupable_array(
    array: pa.Array, date_granularity: Optional[DateGranularity]
) -> pa.Array:
    """Given an input array, return the array we will group by.

    This is for handling DEPRECATED date conversions. The idea is: with input
    value "2021-03-01T21:12:21.231212312Z", a "year" group should be
    "2021-01-01Z".
    """
    if date_granularity is None:
        return array

    if date_granularity == DateGranularity.QUARTER:
        np_datetime_ns = array.to_numpy(zero_copy_only=False)
        np_datetime_m = np_datetime_ns.astype("datetime64[M]").astype(int)
        rounded_month_numbers = np.floor_divide(np_datetime_m, 3) * 3
        np_rounded_ns = rounded_month_numbers.astype("datetime64[M]").astype(
            "datetime64[ns]"
        )
        # converting to int made nulls into ... not-null. Make them null again
        np_rounded_ns[np.isnan(np_datetime_ns)] = "NaT"
        return pa.array(np_rounded_ns)

    if date_granularity == DateGranularity.WEEK:
        # numpy "week" is counted from the Epoch -- which happens to be a
        # Thursday. But ISO weeks start Monday, not Thursday -- and so Numpy's
        # "W" type is useless.
        #
        # We do integer math: add 3 to each date and then floor-divide by 7.
        # That makes "1970-01-01 [Thursday] + 3" => Sunday -- so when we
        # floor-divide, everything from Monday to Sunday falls in the same
        # bucket. We could group by this ... but we convert back to day and
        # subtract the 3, so the group can be formatted.
        np_datetime_ns = array.to_numpy(zero_copy_only=False)
        np_datetime_d = np_datetime_ns.astype("datetime64[D]").astype(int)
        rounded_day_numbers = np.floor_divide(np_datetime_d + 3, 7) * 7 - 3
        np_rounded_ns = rounded_day_numbers.astype("datetime64[D]").astype(
            "datetime64[ns]"
        )
        # converting to int made nulls into ... not-null. Make them null again
        np_rounded_ns[np.isnan(np_datetime_ns)] = "NaT"
        return pa.array(np_rounded_ns)

    freq = date_granularity.numpy_unit
    np_rounded_ns = (
        array.to_numpy(zero_copy_only=False)
        .astype(f"datetime64[{freq}]")
        .astype("datetime64[ns]")
    )
    return pa.array(np_rounded_ns)


def make_sorting_table(table: pa.Table, groups: List[Group]) -> pa.Table:
    """Make the "sorting table": the table we'll sort to detect groups."""
    assert table.columns, "zero-column input cannot use a sorting table"
    assert table.columns[0].num_chunks == 1, "must combine_chunks() before sorting"

    return pa.table(
        {
            group.colname: make_groupable_array(
                table[group.colname].chunks[0], group.date_granularity
            )
            for group in groups
        },
        schema=pa.schema([table.schema.field(group.colname) for group in groups]),
    )


class SortedGroups(NamedTuple):
    sorted_groups: pa.Table
    """Groups: one row per group."""

    sorted_input_table: pa.Table
    """Input table, sorted according to groups.

    Only "needed columns" will be included.
    """

    group_splits: np.array
    """List of indices of "new groups".

    For instance, if sorted_input_table has groups [0, 1, 2], [3] and [4, 5],
    then `group_splits` will be [3, 4].`

    This will be passed to numpy.split.
    Ref: https://numpy.org/doc/stable/reference/generated/numpy.split.html
    """


def find_nonnull_table_mask(table: pa.Table) -> pa.Array:
    mask = pa.array(np.ones(table.num_rows), pa.bool_())

    for column in table.itercolumns():
        mask = pa.compute.and_(mask, column.chunks[0].is_valid())

    return mask


def reencode_dictionary_array(array: pa.Array) -> pa.Array:
    if len(array.indices) <= len(array.dictionary):
        # Groupby often reduces the number of values considerably. Let's shy
        # away from dictionary when it gives us literally nothing.
        return array.cast(pa.utf8())

    used = np.zeros(len(array.dictionary), np.bool_)
    used[array.indices] = True
    if np.all(used):
        return array  # no edit

    return array.cast(pa.utf8()).dictionary_encode()  # TODO optimize


def reencode_dictionaries(table: pa.Table) -> pa.Table:
    for i in range(table.num_columns):
        column = table.columns[i]
        if pa.types.is_dictionary(column.type):
            table = table.set_column(
                i, table.column_names[i], reencode_dictionary_array(column.chunks[0])
            )
    return table


def make_sorted_groups(sorting_table: pa.Table, input_table: pa.Table) -> SortedGroups:
    if not sorting_table.num_columns:
        # Exactly one output group, even for empty-table input
        return SortedGroups(
            sorted_groups=pa.table({"A": [None]}).select([]),  # 1-row, 0-col table
            sorted_input_table=input_table,  # everything is one group (maybe 0-row)
            group_splits=np.array([], np.int64()),
        )

    # pyarrow 3.0.0 can't sort dictionary columns.
    # TODO make sort-dictionary work; nix this conversion
    sorting_table_without_dictionary = pa.table(
        [
            column.cast(pa.utf8()) if pa.types.is_dictionary(column.type) else column
            for column in sorting_table.columns
        ],
        schema=pa.schema(
            [
                pa.field(field.name, pa.utf8())
                if pa.types.is_dictionary(field.type)
                else field
                for field in [
                    sorting_table.schema.field(i)
                    for i in range(len(sorting_table.schema.names))
                ]
            ]
        ),
    )
    indices = pa.compute.sort_indices(
        sorting_table_without_dictionary,
        sort_keys=[
            (c, "ascending") for c in sorting_table_without_dictionary.column_names
        ],
    )

    sorted_groups_with_dups_and_nulls = sorting_table.take(indices)
    # Behavior we ought to DEPRECATE: to mimic Pandas, we drop all groups that
    # contain NULL. This is mathematically sound for Pandas' "NA" (because if
    # all these unknown things are the same thing, doesn't that mean we know
    # something about them? -- reducto ad absurdum, QED). But Workbench's NULL
    # is a bit closer to SQL NULL, which means "whatever you say, pal".
    #
    # This null-dropping is for backwards compat. TODO make it optional ... and
    # eventually nix the option and always output NULL groups.
    nonnull_indices = indices.filter(
        find_nonnull_table_mask(sorted_groups_with_dups_and_nulls)
    )

    if input_table.num_columns:
        sorted_input_table = input_table.take(nonnull_indices)
    else:
        # Don't .take() on a zero-column Arrow table: its .num_rows would change
        #
        # All rows are identical, so .slice() gives the table we want
        sorted_input_table = input_table.slice(0, len(nonnull_indices))

    sorted_groups_with_dups = sorting_table.take(nonnull_indices)

    # "is_dup": find each row in sorted_groups_with_dups that is _equal_ to
    # the row before it. (The first value compares the first and second row.)
    #
    # We start assuming all are equal; then we search for inequality
    if len(sorted_groups_with_dups):
        is_dup = pa.array(np.ones(len(sorted_groups_with_dups) - 1), pa.bool_())
        for column in sorted_groups_with_dups.itercolumns():
            chunk = column.chunks[0]
            if pa.types.is_dictionary(chunk.type):
                chunk = chunk.indices
            first = chunk.slice(0, len(column) - 1)
            second = chunk.slice(1)
            # TODO when we support NULL groups:
            # both_null = pa.compute.and_(first.is_null(), second.is_null())
            # both_equal_if_not_null = pa.compute.equal(first, second)
            # both_equal = pa.compute.fill_null(both_equal_if_not_null, False)
            # value_is_dup = pa.compute.or_(both_null, both_equal)
            # ... and for now, it's simply:
            value_is_dup = pa.compute.equal(first, second)
            is_dup = pa.compute.and_(is_dup, value_is_dup)

        group_splits = np.where(~(is_dup.to_numpy(zero_copy_only=False)))[0] + 1

        sorted_groups = reencode_dictionaries(
            sorted_groups_with_dups.take(np.insert(group_splits, 0, 0))
        )
    else:
        sorted_groups = sorted_groups_with_dups
        group_splits = np.array([], np.int64())

    return SortedGroups(
        sorted_groups=sorted_groups,
        sorted_input_table=sorted_input_table,
        group_splits=group_splits,
    )


def make_table_one_chunk(table: pa.Table) -> pa.Table:
    assert len(table.columns), "Workbench must not give a zero-column table"

    if table.columns[0].num_chunks == 0:
        return pa.table(
            {
                field.name: pa.array([], field.type)
                for field in (
                    table.schema.field(i) for i in range(len(table.schema.names))
                )
            },
            schema=table.schema,
        )

    if table.columns[0].num_chunks == 1:
        return table

    return table.combine_chunks()


def groupby(
    table: pa.Table, groups: List[Group], aggregations: List[Aggregation]
) -> pa.Table:
    simple_table = make_table_one_chunk(table)
    # Pick the "last" of each aggregation for each outname. There will only be
    # one output column with each name.
    aggregations: List[Aggregation] = list(
        reversed({agg.outname: agg for agg in reversed(aggregations)}.values())
    )
    agg_outnames = frozenset((agg.outname for agg in aggregations))
    needed_columns = frozenset((agg.colname for agg in aggregations if agg.colname))
    sorting_table = make_sorting_table(simple_table, groups)

    sorted_groups, sorted_input_table, group_splits = make_sorted_groups(
        sorting_table, table.select(needed_columns)
    )

    retval = sorted_groups.select(
        (
            colname
            for colname in sorted_groups.column_names
            if colname not in agg_outnames
        )
    )
    for agg in aggregations:
        if len(retval) == 0:
            if agg.operation in {Operation.SIZE, Operation.NUNIQUE}:
                field = pa.field(agg.outname, pa.int64(), metadata={"format": "{:,d}"})
            elif agg.operation in {Operation.MEAN, Operation.MEDIAN}:
                field = pa.field(agg.outname, pa.float64(), metadata={"format": "{:,}"})
            else:
                input_field = sorted_input_table.schema.field(agg.colname)
                field = pa.field(
                    agg.outname, input_field.type, metadata=input_field.metadata
                )
            retval = retval.append_column(field, pa.array([], field.type))
        else:
            if agg.operation == Operation.SIZE:
                array = size(
                    num_rows=sorted_input_table.num_rows, group_splits=group_splits
                )
                field = pa.field(agg.outname, array.type, metadata={"format": "{:,d}"})
            elif agg.operation == Operation.NUNIQUE:
                array = nunique(
                    array=sorted_input_table[agg.colname].chunks[0],
                    group_splits=group_splits,
                )
                field = pa.field(agg.outname, array.type, metadata={"format": "{:,d}"})
            else:
                ufunc = dict(
                    sum=sum,
                    first=first,
                    mean=mean,
                    median=median,
                    min=min,
                    max=max,
                    nunique=nunique,
                )[agg.operation.value]
                array = sorted_input_table[agg.colname].chunks[0]
                if pa.types.is_dictionary(sorted_input_table[agg.colname].type):
                    array = array.cast(pa.utf8())
                array = ufunc(
                    array=array,
                    group_splits=group_splits,
                )
                if pa.types.is_dictionary(sorted_input_table[agg.colname].type):
                    array = array.cast(pa.utf8()).dictionary_encode()
                input_field = sorted_input_table.schema.field(agg.colname)
                if (
                    agg.operation
                    in {
                        Operation.MEAN,
                        Operation.MEDIAN,
                    }
                    and not pa.types.is_floating(input_field.type)
                ):
                    metadata = {"format": "{:,}"}  # float default
                else:
                    metadata = input_field.metadata
                field = pa.field(agg.outname, array.type, metadata=metadata)
            retval = retval.append_column(field, array)

    return retval


def _timestamp_is_rounded(
    column: pa.ChunkedArray, granularity: DateGranularity
) -> bool:
    factor = {
        DateGranularity.SECOND: 1_000_000_000,
        DateGranularity.MINUTE: 1_000_000_000 * 60,
        DateGranularity.HOUR: 1_000_000_000 * 60 * 60,
    }[granularity]
    ints = column.cast(pa.int64())
    return pa.compute.all(
        pa.compute.equal(
            ints, pa.compute.multiply(pa.compute.divide(ints, factor), factor)
        )
    ).as_py()


def _warn_if_using_deprecated_date_granularity(
    table: pa.Table, groups: List[Group]
) -> List[RenderError]:
    errors = []

    deprecated_need_upgrade_to_date: List[Group] = []
    deprecated_need_timestampmath: List[Group] = []
    for group in groups:
        if group.date_granularity is not None and pa.types.is_timestamp(
            table.schema.field(group.colname).type
        ):
            if group.date_granularity in {
                DateGranularity.DAY,
                DateGranularity.WEEK,
                DateGranularity.MONTH,
                DateGranularity.QUARTER,
                DateGranularity.YEAR,
            }:
                deprecated_need_upgrade_to_date.append(group)
            elif not _timestamp_is_rounded(
                table[group.colname], group.date_granularity
            ):
                deprecated_need_timestampmath.append(group)

    if deprecated_need_upgrade_to_date:
        errors.append(
            RenderError(
                i18n.trans(
                    "group_dates.granularity_deprecated.need_dates",
                    "The “Group Dates” feature has changed. Please click to upgrade from Timestamps to Dates. Workbench will force-upgrade in January 2022.",
                ),
                quick_fixes=[
                    QuickFix(
                        i18n.trans(
                            "group_dates.granularity_deprecated.quick_fix.convert_to_date",
                            "Upgrade",
                        ),
                        QuickFixAction.PrependStep(
                            "converttimestamptodate",
                            dict(
                                colnames=[group.colname],
                                unit=group.date_granularity.date_unit,
                            ),
                        ),
                    )
                    for group in deprecated_need_upgrade_to_date
                ],
            )
        )

    if deprecated_need_timestampmath:
        errors.append(
            RenderError(
                i18n.trans(
                    "group_dates.granularity_deprecated.need_rounding",
                    "The “Group Dates” feature has changed. Please click to upgrade to Timestamp Math. Workbench will force-upgrade in January 2022.",
                ),
                quick_fixes=[
                    QuickFix(
                        i18n.trans(
                            "group_dates.granularity_deprecated.quick_fix.round_timestamps",
                            "Upgrade",
                        ),
                        QuickFixAction.PrependStep(
                            "timestampmath",
                            dict(
                                colnames=[group.colname],
                                operation="startof",
                                roundunit=group.date_granularity.rounding_unit,
                            ),
                        ),
                    )
                    for group in deprecated_need_timestampmath
                ],
            )
        )

    return errors


def _warn_to_suggest_convert_to_date(
    schema: pa.Schema, colnames: FrozenSet[str]
) -> List[RenderError]:
    fields = [field for field in schema if field.name in colnames]

    # No warnings if the user is already grouping by date
    if any((pa.types.is_date32(field.type) for field in fields)):
        return []

    # No warnings if the user did not select a column
    if not fields:
        return []

    errors = []

    timestamp_colnames = []
    text_colnames = []
    for field in fields:
        if pa.types.is_timestamp(field.type):
            timestamp_colnames.append(field.name)
        elif pa.types.is_string(field.type) or pa.types.is_dictionary(field.type):
            text_colnames.append(field.name)

    if timestamp_colnames:
        errors.append(
            RenderError(
                i18n.trans(
                    "group_dates.timestamp_selected",
                    "{columns, plural, offset:1 =1 {“{column0}” is Timestamp.}=2 {“{column0}” and one other column are Timestamp.}other {“{column0}” and # other columns are Timestamp.}}",
                    dict(
                        columns=len(timestamp_colnames), column0=timestamp_colnames[0]
                    ),
                ),
                [
                    QuickFix(
                        i18n.trans(
                            "group_dates.quick_fix.convert_timestamp_to_date",
                            "Convert to Date",
                        ),
                        QuickFixAction.PrependStep(
                            "converttimestamptodate", dict(colnames=timestamp_colnames)
                        ),
                    )
                ],
            )
        )
    elif text_colnames:
        errors.append(
            RenderError(
                i18n.trans(
                    "group_dates.text_selected",
                    "{columns, plural, offset:1 =1 {“{column0}” is Text.}=2 {“{column0}” and one other column are Text.}other {“{column0}” and # other columns are Text.}}",
                    dict(columns=len(text_colnames), column0=text_colnames[0]),
                ),
                [
                    QuickFix(
                        i18n.trans(
                            "group_dates.quick_fix.convert_text_to_date",
                            "Convert to Date",
                        ),
                        QuickFixAction.PrependStep(
                            "converttexttodate", dict(colnames=text_colnames)
                        ),
                    ),
                    QuickFix(
                        i18n.trans(
                            "group_dates.quick_fix.convert_text_to_timestamp",
                            "Convert to Timestamp first",
                        ),
                        QuickFixAction.PrependStep(
                            "convert-date", dict(colnames=text_colnames)
                        ),
                    ),
                ],
            )
        )
    else:
        errors.append(
            RenderError(
                i18n.trans(
                    "group_dates.select_date_columns",
                    "Select a Date column, or uncheck “Group Dates”.",
                )
            )
        )

    return errors


def render_arrow_v1(
    table: pa.Table, params: Dict[str, Any], **kwargs
) -> ArrowRenderResult:
    colnames = table.column_names
    date_colnames = frozenset(
        colname for colname in colnames if pa.types.is_timestamp(table[colname].type)
    )
    groups = parse_groups(date_colnames=date_colnames, **params["groups"])
    aggregations = parse_aggregations(params["aggregations"])

    # HACK: set the same default aggregations as we do in our JavaScript component.
    if not aggregations:
        aggregations.append(
            Aggregation(Operation.SIZE, "", Operation.SIZE.default_outname(""))
        )

    # This is a "Group By" module so we need to support the obvious operation,
    # 'SELECT COUNT(*) FROM input'. The obvious way to display that is to select
    # "Count" and not select a Group By column.
    #
    # ... and unfortunately, that form setup -- no columns selected, one
    # "Count" aggregation selected -- is exactly what the user sees by default
    # after adding the module, before step 1 of the onboarding path.
    #
    # So we get a tough choice: either make "no aggregations" a no-op to give
    # us the ideal onboarding path, _OR_ make "no aggregations" default to
    # "count", to support the obvious operation. Pick one: complete+simple, or
    # onboarding-friendly.
    #
    # For now, we're onboarding-friendly and we don't allow SELECT COUNT(*).
    # When we solve https://www.pivotaltracker.com/story/show/163264164 we
    # should change to be complete+simple (because the onboarding will have
    # another answer). That's
    # https://www.pivotaltracker.com/story/show/164375318
    if not groups and aggregations == [
        Aggregation(Operation.SIZE, "", Operation.SIZE.default_outname(""))
    ]:
        return ArrowRenderResult(table)  # no-op: users haven't entered any params

    # Error out with a quickfix if aggregations need number and we're not number
    non_numeric_colnames = []
    for aggregation in aggregations:
        if aggregation.operation.needs_numeric_column():
            colname = aggregation.colname
            column = table[colname]
            if (
                not pa.types.is_integer(column.type)
                and not pa.types.is_floating(column.type)
            ) and colname not in non_numeric_colnames:
                non_numeric_colnames.append(colname)
    if non_numeric_colnames:
        return ArrowRenderResult(
            pa.table({}),
            errors=[
                RenderError(
                    i18n.trans(
                        "non_numeric_colnames.error",
                        "{n_columns, plural,"
                        ' one {Column "{first_colname}"}'
                        ' other {# columns (see "{first_colname}")}} '
                        "must be Numbers",
                        {
                            "n_columns": len(non_numeric_colnames),
                            "first_colname": non_numeric_colnames[0],
                        },
                    ),
                    quick_fixes=[
                        QuickFix(
                            i18n.trans(
                                "non_numeric_colnames.quick_fix.text", "Convert"
                            ),
                            action=QuickFixAction.PrependStep(
                                "converttexttonumber",
                                {"colnames": non_numeric_colnames},
                            ),
                        )
                    ],
                )
            ],
        )

    errors = _warn_if_using_deprecated_date_granularity(table, groups)
    if not errors and params["groups"]["group_dates"]:
        errors = _warn_to_suggest_convert_to_date(
            table.schema, frozenset(group.colname for group in groups)
        )

    result_table = groupby(table, groups, aggregations)
    return ArrowRenderResult(result_table, errors=errors)
