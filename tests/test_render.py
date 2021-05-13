import datetime
from pathlib import Path

from cjwmodule.arrow.testing import assert_result_equals, make_column, make_table
from cjwmodule.arrow.types import ArrowRenderResult
from cjwmodule.spec.testing import param_factory
from cjwmodule.testing.i18n import i18n_message
from cjwmodule.types import QuickFix, QuickFixAction, RenderError

from groupby import render_arrow_v1 as render

P = param_factory(Path(__file__).parent.parent / "groupby.yaml")


# def test_defaults_count():
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


def test_count_no_colnames_is_no_op_TEMPORARY():
    # Added for https://www.pivotaltracker.com/story/show/164375369
    # Change behavior for https://www.pivotaltracker.com/story/show/164375318
    assert_result_equals(
        render(
            make_table(make_column("A", [1, 2])),
            P(
                groups=dict(colnames=[], group_dates=False, date_granularities={}),
                aggregations=[],
            ),
        ),
        ArrowRenderResult(make_table(make_column("A", [1, 2]))),
    )


def test_count_with_colnames():
    # Check for obvious bug when adding
    # https://www.pivotaltracker.com/story/show/164375369
    assert_result_equals(
        render(
            make_table(make_column("A", [1, 2])),
            P(
                groups=dict(colnames=["A"], group_dates=False, date_granularities={}),
                aggregations=[],
            ),
        ),
        ArrowRenderResult(
            make_table(
                make_column("A", [1, 2]),
                make_column("Group Size", [1, 1], format="{:,d}"),
            )
        ),
    )


def test_default_outnames():
    assert_result_equals(
        render(
            make_table(
                make_column("A", ["x", "x"]), make_column("B", [1, 2], format="{:d}")
            ),
            P(
                groups=dict(colnames=["A"], group_dates=False, date_granularities={}),
                aggregations=[
                    dict(operation="size", colname="", outname=""),
                    dict(operation="nunique", colname="B", outname=""),
                    dict(operation="sum", colname="B", outname=""),
                    dict(operation="mean", colname="B", outname=""),
                    dict(operation="median", colname="B", outname=""),
                    dict(operation="min", colname="B", outname=""),
                    dict(operation="max", colname="B", outname=""),
                    dict(operation="first", colname="B", outname=""),
                ],
            ),
        ),
        ArrowRenderResult(
            make_table(
                make_column("A", ["x"]),
                make_column("Group Size", [2], format="{:,d}"),
                make_column("Unique count of B", [2], format="{:,d}"),
                make_column("Sum of B", [3], format="{:d}"),
                make_column("Average of B", [1.5], format="{:,}"),
                make_column("Median of B", [1.5], format="{:,}"),
                make_column("Minimum of B", [1], format="{:d}"),
                make_column("Maximum of B", [2], format="{:d}"),
                make_column("First of B", [1], format="{:d}"),
            )
        ),
    )


def test_quickfix_convert_value_strings_to_numbers():
    assert_result_equals(
        render(
            make_table(
                make_column("A", [1, 1, 1]),
                make_column("B", ["a", "b", "a"]),
                make_column("C", ["a", "b", "a"]),
            ),
            P(
                groups=dict(colnames=["A"], group_dates=False, date_granularities={}),
                aggregations=[
                    dict(operation="mean", colname="B", outname="mean"),
                    dict(operation="sum", colname="C", outname="sum"),
                ],
            ),
        ),
        ArrowRenderResult(
            make_table(),
            [
                RenderError(
                    i18n_message(
                        "non_numeric_colnames.error",
                        {"n_columns": 2, "first_colname": "B"},
                    ),
                    quick_fixes=[
                        QuickFix(
                            i18n_message("non_numeric_colnames.quick_fix.text"),
                            QuickFixAction.PrependStep(
                                "converttexttonumber", {"colnames": ["B", "C"]}
                            ),
                        )
                    ],
                )
            ],
        ),
    )


def test_ignore_aggregation_with_empty_colname():
    # Workbench replaces non-existent column names with "". So we can end up
    # running groupby with aggregations that have no column.
    #
    # These appear in the UI, so users can select new columns. But we won't
    # render them.
    assert_result_equals(
        render(
            make_table(make_column("A", [1])),
            P(
                groups=dict(colnames=[], group_dates=False, date_granularities={}),
                aggregations=[
                    dict(operation="size", colname="", outname="size"),
                    dict(operation="sum", colname="", outname="sum"),
                ],
            ),
        ),
        ArrowRenderResult(make_table(make_column("size", [1], format="{:,d}"))),
    )


def test_ignore_non_date_timestamps():
    # Steps for the user to get here:
    # 1. Make a date column, 'A'
    # 2. Check "Group Dates". The column appears.
    # 3. Select column 'A', and select a date granularity for it
    # 4. Alter the input DataFrame such that 'A' is no longer datetime
    #
    # Expected results: you can't group it by date any more.
    assert_result_equals(
        render(
            make_table(
                make_column("A", [1]),  # "used to be a datetime"
                make_column(
                    "B", [datetime.datetime(2019, 1, 4)]
                ),  # so we don't need quickfix
            ),
            P(
                groups=dict(
                    colnames=["A"], group_dates=True, date_granularities={"A": "T"}
                ),
                aggregations=[dict(operation="size", colname="", outname="size")],
            ),
        ),
        ArrowRenderResult(
            make_table(make_column("A", [1]), make_column("size", [1], format="{:,d}")),
            [RenderError(i18n_message("group_dates.select_date_columns"))],
        ),
    )


def test_group_dates_prompt_select_date_column():
    assert_result_equals(
        render(
            make_table(make_column("A", [1])),
            P(
                groups=dict(colnames=["A"], group_dates=True, date_granularities={}),
                aggregations=[dict(operation="size", colname="", outname="size")],
            ),
        ),
        ArrowRenderResult(
            make_table(make_column("A", [1]), make_column("size", [1], format="{:,d}")),
            errors=[RenderError(i18n_message("group_dates.select_date_columns"))],
        ),
    )


def test_group_date_no_errors_when_nothing_selected():
    assert_result_equals(
        render(
            make_table(make_column("A", [1])),
            P(
                groups=dict(colnames=[], group_dates=True, date_granularities={}),
                aggregations=[dict(operation="sum", colname="A", outname="sum")],
            ),
        ),
        ArrowRenderResult(make_table(make_column("sum", [1]))),
    )


def test_group_date_no_errors_when_date_selected():
    assert_result_equals(
        render(
            make_table(
                make_column("A", [datetime.date(2021, 5, 5)], unit="day"),
                make_column("B", [1]),
            ),
            P(
                groups=dict(
                    colnames=["A", "B"], group_dates=True, date_granularities={}
                ),
                aggregations=[dict(operation="size", colname="", outname="size")],
            ),
        ),
        ArrowRenderResult(
            make_table(
                make_column("A", [datetime.date(2021, 5, 5)], unit="day"),
                make_column("B", [1]),
                make_column("size", [1], format="{:,d}"),
            ),
        ),
    )


def test_group_date_prompt_convert_timestamp_to_date():
    assert_result_equals(
        render(
            make_table(make_column("A", [datetime.datetime(2021, 5, 5)])),
            P(
                groups=dict(colnames=["A"], group_dates=True, date_granularities={}),
                aggregations=[dict(operation="size", colname="", outname="size")],
            ),
        ),
        ArrowRenderResult(
            make_table(
                make_column("A", [datetime.datetime(2021, 5, 5)]),
                make_column("size", [1], format="{:,d}"),
            ),
            [
                RenderError(
                    i18n_message(
                        "group_dates.timestamp_selected", dict(columns=1, column0="A")
                    ),
                    [
                        QuickFix(
                            i18n_message(
                                "group_dates.quick_fix.convert_timestamp_to_date"
                            ),
                            QuickFixAction.PrependStep(
                                "converttimestamptodate", dict(colnames=["A"])
                            ),
                        )
                    ],
                )
            ],
        ),
    )


def test_group_date_prompt_convert_text_to_date():
    assert_result_equals(
        render(
            make_table(
                make_column("A", ["2021-05-05"]),
                make_column("B", ["2021-05-05"]),
            ),
            P(
                groups=dict(
                    colnames=["A", "B"], group_dates=True, date_granularities={}
                ),
                aggregations=[dict(operation="size", colname="", outname="size")],
            ),
        ),
        ArrowRenderResult(
            make_table(
                make_column("A", ["2021-05-05"]),
                make_column("B", ["2021-05-05"]),
                make_column("size", [1], format="{:,d}"),
            ),
            [
                RenderError(
                    i18n_message(
                        "group_dates.text_selected", dict(columns=2, column0="A")
                    ),
                    [
                        QuickFix(
                            i18n_message("group_dates.quick_fix.convert_text_to_date"),
                            QuickFixAction.PrependStep(
                                "converttexttodate", dict(colnames=["A", "B"])
                            ),
                        ),
                        QuickFix(
                            i18n_message(
                                "group_dates.quick_fix.convert_text_to_timestamp"
                            ),
                            QuickFixAction.PrependStep(
                                "convert-date", dict(colnames=["A", "B"])
                            ),
                        ),
                    ],
                )
            ],
        ),
    )


def test_group_date_prompt_upgrade_timestamp_to_date():
    assert_result_equals(
        render(
            make_table(make_column("A", [datetime.datetime(2021, 5, 5)])),
            P(
                groups=dict(
                    colnames=["A"], group_dates=True, date_granularities={"A": "Y"}
                ),
                aggregations=[dict(operation="size", colname="", outname="size")],
            ),
        ),
        ArrowRenderResult(
            make_table(
                make_column("A", [datetime.datetime(2021, 1, 1)]),
                make_column("size", [1], format="{:,d}"),
            ),
            [
                RenderError(
                    i18n_message("group_dates.granularity_deprecated.need_dates"),
                    [
                        QuickFix(
                            i18n_message(
                                "group_dates.granularity_deprecated.quick_fix.convert_to_date"
                            ),
                            QuickFixAction.PrependStep(
                                "converttimestamptodate",
                                dict(colnames=["A"], unit="year"),
                            ),
                        )
                    ],
                )
            ],
        ),
    )


def test_group_date_prompt_upgrade_timestampmath():
    assert_result_equals(
        render(
            make_table(make_column("A", [datetime.datetime(2021, 5, 5, 1, 2, 3, 4)])),
            P(
                groups=dict(
                    colnames=["A"], group_dates=True, date_granularities={"A": "S"}
                ),
                aggregations=[dict(operation="size", colname="", outname="size")],
            ),
        ),
        ArrowRenderResult(
            make_table(
                make_column("A", [datetime.datetime(2021, 5, 5, 1, 2, 3)]),
                make_column("size", [1], format="{:,d}"),
            ),
            [
                RenderError(
                    i18n_message("group_dates.granularity_deprecated.need_rounding"),
                    [
                        QuickFix(
                            i18n_message(
                                "group_dates.granularity_deprecated.quick_fix.round_timestamps"
                            ),
                            QuickFixAction.PrependStep(
                                "timestampmath",
                                dict(
                                    colnames=["A"],
                                    operation="startof",
                                    roundunit="second",
                                ),
                            ),
                        )
                    ],
                )
            ],
        ),
    )
