from pathlib import Path

from cjwmodule.spec.testing import param_factory

from groupby import migrate_params

P = param_factory(Path(__file__).parent.parent / "groupby.json")


v1_defaults = {
    "groupby|groupby|0": "",
    "groupby|groupby|1": "",
    "active.addremove.last|groupby|1": False,
    "operation|operation|0": 0,
    "targetcolumn|operation|0": "",
    "outputname|operation|0": "",
    "operation.show-sibling|operation|1": 0,
    "targetcolumn.hide-with-sibling|operation|1": "",
    "outputname|operation|1": "",
    "cheat.cheat|operation|1": False,
    "active.addremove|operation|1": False,
    "operation.show-sibling|operation|2": 0,
    "targetcolumn.hide-with-sibling|operation|2": "",
    "outputname|operation|2": "",
    "cheat.cheat|operation|2": False,
    "active.addremove|operation|2": False,
    "operation.show-sibling|operation|3": 0,
    "targetcolumn.hide-with-sibling|operation|3": "",
    "outputname|operation|3": "",
    "cheat.cheat|operation|3": False,
    "active.addremove|operation|3": False,
    "operation.show-sibling|operation|4": 0,
    "targetcolumn.hide-with-sibling|operation|4": "",
    "outputname|operation|4": "",
    "cheat.cheat|operation|4": False,
    "active.addremove.last|operation|4": False,
}


def test_migrate_v1_default():
    assert migrate_params(v1_defaults) == P(
        groups={
            "colnames": [],
            "group_dates": False,
            "date_granularities": {},
        },
        aggregations=[{"operation": "size", "colname": "", "outname": ""}],
    )


def test_migrate_v1_two_colnames():
    assert (
        migrate_params(
            {
                **v1_defaults,
                "groupby|groupby|0": "a",
                "groupby|groupby|1": "b",
                "active.addremove.last|groupby|1": True,
            }
        )["groups"]["colnames"]
        == ["a", "b"]
    )


def test_migrate_v1_two_colnames_but_second_not_active():
    assert (
        migrate_params(
            {
                **v1_defaults,
                "groupby|groupby|0": "a",
                "groupby|groupby|1": "b",
                "active.addremove.last|groupby|1": False,
            }
        )["groups"]["colnames"]
        == ["a"]
    )


def test_migrate_v1_aggregation():
    assert (
        migrate_params(
            {
                **v1_defaults,
                "operation|operation|0": 5,
                "targetcolumn|operation|0": "c",
                "outputname|operation|0": "C",
            }
        )["aggregations"]
        == [{"operation": "max", "colname": "c", "outname": "C"}]
    )


def test_migrate_v1_only_active_aggregations():
    assert (
        migrate_params(
            {
                **v1_defaults,
                "operation|operation|0": 5,
                "targetcolumn|operation|0": "c",
                "outputname|operation|0": "C",
                "active.addremove|operation|1": False,
                # The next operation isn't active, so it will be ignored
                "operation.show-sibling|operation|1": 2,
                "targetcolumn.hide-with-sibling|operation|1": "d",
                "outputname|operation|1": "D",
            }
        )["aggregations"]
        == [{"operation": "max", "colname": "c", "outname": "C"}]
    )


def test_migrate_v1_many_aggregations():
    assert migrate_params(
        {
            **v1_defaults,
            # COUNT(*) AS A
            "operation|operation|0": 0,
            "targetcolumn|operation|0": "",
            "outputname|operation|0": "A",
            # COUNT DISTINCT(*) AS B
            "active.addremove|operation|1": True,
            "operation.show-sibling|operation|1": 1,
            "targetcolumn.hide-with-sibling|operation|1": "",
            "outputname|operation|1": "B",
            # SUM(c) AS C
            "active.addremove|operation|2": True,
            "operation.show-sibling|operation|2": 2,
            "targetcolumn.hide-with-sibling|operation|2": "c",
            "outputname|operation|2": "C",
            # MEAN(d) AS D
            "active.addremove|operation|3": True,
            "operation.show-sibling|operation|3": 3,
            "targetcolumn.hide-with-sibling|operation|3": "d",
            "outputname|operation|3": "D",
            # MAX(e) AS E
            "active.addremove|operation|4": True,
            "operation.show-sibling|operation|4": 4,
            "targetcolumn.hide-with-sibling|operation|4": "e",
            "outputname|operation|4": "E",
        }
    )["aggregations"] == [
        {"operation": "size", "colname": "", "outname": "A"},
        {"operation": "nunique", "colname": "", "outname": "B"},
        {"operation": "sum", "colname": "c", "outname": "C"},
        {"operation": "mean", "colname": "d", "outname": "D"},
        {"operation": "min", "colname": "e", "outname": "E"},
    ]


def test_migrate_v1_omit_aggregation_missing_colname():
    assert (
        migrate_params(
            {
                **v1_defaults,
                # SUM(*) AS A (which isn't valid)
                "operation|operation|0": 2,
                "targetcolumn|operation|0": "",
                "outputname|operation|0": "A",
                # COUNT DISTINCT(*) AS B
                "active.addremove|operation|1": True,
                "operation.show-sibling|operation|1": 1,
                "targetcolumn.hide-with-sibling|operation|1": "",
                "outputname|operation|1": "B",
            }
        )["aggregations"]
        == [{"operation": "nunique", "colname": "", "outname": "B"}]
    )


def test_migrate_v2_no_colnames():
    assert migrate_params(
        {
            "groups": {
                "colnames": "",
                "group_dates": False,
                "date_granularities": {},
            },
            "aggregations": [],
        }
    ) == P(
        groups={
            "colnames": [],
            "group_dates": False,
            "date_granularities": {},
        },
        aggregations=[],
    )


def test_migrate_v2():
    assert migrate_params(
        {
            "groups": {
                "colnames": "A,B",
                "group_dates": False,
                "date_granularities": {},
            },
            "aggregations": [],
        }
    ) == P(
        groups={
            "colnames": ["A", "B"],
            "group_dates": False,
            "date_granularities": {},
        },
        aggregations=[],
    )


def test_migrate_v3():
    assert migrate_params(
        {
            "groups": {
                "colnames": ["A", "B"],
                "group_dates": False,
                "date_granularities": {},
            },
            "aggregations": [],
        }
    ) == P(
        groups={
            "colnames": ["A", "B"],
            "group_dates": False,
            "date_granularities": {},
        },
        aggregations=[],
    )
