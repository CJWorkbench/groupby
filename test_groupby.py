import unittest
import pandas as pd
import numpy as np
from groupby import render

defaultparams = {
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
  "active.addremove.last|operation|4": False
}

class TestFilter(unittest.TestCase):

    def setUp(self):
        # Test data includes some partially and completely empty rows because this tends to freak out Pandas
        self.table = pd.DataFrame([
            ['bread', 'liberty', 3, 'round', '2018-1-12', 2],
            ['bread', 'liberty', None, 'square', '2018-1-12 08:15', 1],
            ['bread', 'death', 3, 'round', '2018-1-12', 5],
            ['bread', 'death', 4, 'square', '2018-1-12 08:15', 8],
            [None, None, None, None, None, None],
            ['roses', 'liberty', 10, 'Round', '2015-7-31', 7],
            ['roses', 'liberty', 11, 'square', '2018-3-12', 4],
            ['roses', 'death', None, 'square', '2018-3-12', 9]
        ], columns=['a', 'b', 'c', 'd', 'date', 'e'])

    def test_no_params(self):
        out = render(self.table, defaultparams)
        self.assertTrue(out.equals(self.table))  # should NOP when first applied

    def test_one_level(self):
        param_copy = defaultparams.copy()
        param_copy['groupby|groupby|0'] = "a"
        out_table = pd.DataFrame([
            ['bread', 4],
            ['roses', 3]
        ], columns=['a', 'Group Size'])
        out = render(self.table, param_copy)
        self.assertTrue(out.equals(out_table))

    # #157104264: selecting non-count operation, setting a string target column, then switching to count caused error
    def test_count_with_targetcolumn(self):
        param_copy = defaultparams.copy()
        param_copy['groupby|groupby|0'] = "a"
        param_copy['targetcolumn|operation|0'] = "b" # string type
        out_table = pd.DataFrame([
            ['bread', 4],
            ['roses', 3]
        ], columns=['a', 'Group Size'])
        out = render(self.table, param_copy)
        self.assertTrue(out.equals(out_table))

    def test_two_level(self):
        param_copy = defaultparams.copy()
        param_copy['groupby|groupby|0'] = "a"
        param_copy['groupby|groupby|1'] = "b"
        param_copy['active.addremove.last|groupby|1'] = True
        out_table = pd.DataFrame([
            ['bread', 'death', 2],
            ['bread', 'liberty', 2],
            ['roses', 'death', 1],
            ['roses', 'liberty', 2],
        ], columns=['a', 'b', 'Group Size'])
        out = render(self.table, param_copy)
        self.assertTrue(out.equals(out_table))

    def test_one_level_avg(self):
        param_copy = defaultparams.copy()
        param_copy['groupby|groupby|0'] = "a"
        param_copy['operation|operation|0'] = 3
        param_copy['targetcolumn|operation|0'] = "c"
        out_table = pd.DataFrame([
            ['bread', float(sum([3, 3, 4])/3)],
            ['roses', float(sum([10, 11])/2)]
        ], columns=['a', 'Average of c'])
        out = render(self.table, param_copy)
        self.assertTrue(out.equals(out_table))

    def test_two_level_avg(self):
        param_copy = defaultparams.copy()
        param_copy['groupby|groupby|0'] = "a"
        param_copy['groupby|groupby|1'] = "b"
        param_copy['active.addremove.last|groupby|1'] = True
        param_copy['operation|operation|0'] = 3
        param_copy['targetcolumn|operation|0'] = "c"
        out_table = pd.DataFrame([
            ['bread', 'death', float(sum([3, 4])/2)],
            ['bread', 'liberty', float(3)],
            ['roses', 'death', float('nan')],
            ['roses', 'liberty', float(sum([10, 11]) / 2)],
        ], columns=['a', 'b', 'Average of c'])
        out = render(self.table, param_copy)
        self.assertTrue(out.equals(out_table))

    def test_one_level_sum(self):
        param_copy = defaultparams.copy()
        param_copy['groupby|groupby|0'] = "a"
        param_copy['operation|operation|0'] = 2
        param_copy['targetcolumn|operation|0'] = "c"
        out_table = pd.DataFrame([
            ['bread', float(10)],
            ['roses', float(21)]
        ], columns=['a', 'Sum of c'])
        out = render(self.table, param_copy)
        self.assertTrue(out.equals(out_table))

    def test_two_level_sum(self):
        param_copy = defaultparams.copy()
        param_copy['groupby|groupby|0'] = "a"
        param_copy['groupby|groupby|1'] = "b"
        param_copy['active.addremove.last|groupby|1'] = True
        param_copy['operation|operation|0'] = 2
        param_copy['targetcolumn|operation|0'] = "c"
        out_table = pd.DataFrame([
            ['bread', 'death', float(7)],
            ['bread', 'liberty', float(3)],
            ['roses', 'death', float('nan')],
            ['roses', 'liberty', float(21)],
        ], columns=['a', 'b', 'Sum of c'])
        out = render(self.table, param_copy)
        self.assertTrue(out.equals(out_table))

    def test_one_level_min(self):
        param_copy = defaultparams.copy()
        param_copy['groupby|groupby|0'] = "a"
        param_copy['operation|operation|0'] = 4
        param_copy['targetcolumn|operation|0'] = "c"
        out_table = pd.DataFrame([
            ['bread', float(3)],
            ['roses', float(10)]
        ], columns=['a', 'Min of c'])
        out = render(self.table, param_copy)
        self.assertTrue(out.equals(out_table))

    def test_two_level_min(self):
        param_copy = defaultparams.copy()
        param_copy['groupby|groupby|0'] = "a"
        param_copy['groupby|groupby|1'] = "b"
        param_copy['active.addremove.last|groupby|1'] = True
        param_copy['operation|operation|0'] = 4
        param_copy['targetcolumn|operation|0'] = "c"
        out_table = pd.DataFrame([
            ['bread', 'death', float(3)],
            ['bread', 'liberty', float(3)],
            ['roses', 'death', float('nan')],
            ['roses', 'liberty', float(10)],
        ], columns=['a', 'b', 'Min of c'])
        out = render(self.table, param_copy)
        self.assertTrue(out.equals(out_table))

    def test_one_level_max(self):
        param_copy = defaultparams.copy()
        param_copy['groupby|groupby|0'] = "a"
        param_copy['operation|operation|0'] = 5
        param_copy['targetcolumn|operation|0'] = "c"
        out_table = pd.DataFrame([
            ['bread', float(4)],
            ['roses', float(11)]
        ], columns=['a', 'Max of c'])
        out = render(self.table, param_copy)
        self.assertTrue(out.equals(out_table))

    def test_two_level_max(self):
        param_copy = defaultparams.copy()
        param_copy['groupby|groupby|0'] = "a"
        param_copy['groupby|groupby|1'] = "b"
        param_copy['active.addremove.last|groupby|1'] = True
        param_copy['operation|operation|0'] = 5
        param_copy['targetcolumn|operation|0'] = "c"
        out_table = pd.DataFrame([
            ['bread', 'death', float(4)],
            ['bread', 'liberty', float(3)],
            ['roses', 'death', float('nan')],
            ['roses', 'liberty', float(11)],
        ], columns=['a', 'b', 'Max of c'])
        out = render(self.table, param_copy)
        self.assertTrue(out.equals(out_table))

    def test_one_level_count_unique(self):
        param_copy = defaultparams.copy()
        param_copy['groupby|groupby|0'] = "a"
        param_copy['operation|operation|0'] = 1
        param_copy['targetcolumn|operation|0'] = "d"
        out_table = pd.DataFrame([
            ['bread', 2],
            ['roses', 2]
        ], columns=['a', 'Count unique of d'])
        out = render(self.table, param_copy)
        self.assertTrue(out.equals(out_table))

    def test_two_level_count_unique(self):
        param_copy = defaultparams.copy()
        param_copy['groupby|groupby|0'] = "a"
        param_copy['groupby|groupby|1'] = "b"
        param_copy['active.addremove.last|groupby|1'] = True
        param_copy['operation|operation|0'] = 1
        param_copy['targetcolumn|operation|0'] = "d"
        out_table = pd.DataFrame([
            ['bread', 'death', 2],
            ['bread', 'liberty', 2],
            ['roses', 'death', 1],
            ['roses', 'liberty', 2],
        ], columns=['a', 'b', 'Count unique of d'])
        out = render(self.table, param_copy)
        self.assertTrue(out.equals(out_table))

    def test_two_level_multi_op(self):
        param_copy = defaultparams.copy()
        param_copy['groupby|groupby|0'] = "a"
        param_copy['groupby|groupby|1'] = "b"
        param_copy['active.addremove.last|groupby|1'] = True
        param_copy['operation|operation|0'] = 5
        param_copy['targetcolumn|operation|0'] = "c"
        param_copy['active.addremove|operation|1'] = True
        out_table = pd.DataFrame([
            ['bread', 'death', float(4), 2],
            ['bread', 'liberty', float(3), 2],
            ['roses', 'death', float('nan'), 1],
            ['roses', 'liberty', float(11), 2],
        ], columns=['a', 'b', 'Max of c', 'Group Size'])
        out = render(self.table, param_copy)
        self.assertTrue(out.equals(out_table))

    def test_two_level_multi_op_no_target(self):
        param_copy = defaultparams.copy()
        param_copy['groupby|groupby|0'] = "a"
        param_copy['groupby|groupby|1'] = "b"
        param_copy['active.addremove.last|groupby|1'] = True
        param_copy['operation|operation|0'] = 5
        param_copy['targetcolumn|operation|0'] = "c"
        param_copy['operation.show-sibling|operation|1'] = 2
        param_copy['active.addremove|operation|1'] = True
        out_table = pd.DataFrame([
            ['bread', 'death', float(4)],
            ['bread', 'liberty', float(3)],
            ['roses', 'death', float('nan')],
            ['roses', 'liberty', float(11)],
        ], columns=['a', 'b', 'Max of c'])
        out = render(self.table, param_copy)
        self.assertTrue(out.equals(out_table))

    def test_two_level_multi_op_same_target(self):
        param_copy = defaultparams.copy()
        param_copy['groupby|groupby|0'] = "a"
        param_copy['groupby|groupby|1'] = "b"
        param_copy['active.addremove.last|groupby|1'] = True
        param_copy['operation|operation|0'] = 5
        param_copy['targetcolumn|operation|0'] = "c"
        param_copy['operation.show-sibling|operation|1'] = 4
        param_copy['targetcolumn.hide-with-sibling|operation|1'] = "c"
        param_copy['active.addremove|operation|1'] = True
        out_table = pd.DataFrame([
            ['bread', 'death', float(4), float(3)],
            ['bread', 'liberty', float(3), float(3)],
            ['roses', 'death', float('nan'), float('nan')],
            ['roses', 'liberty', float(11), float(10)],
        ], columns=['a', 'b', 'Max of c', 'Min of c'])
        out = render(self.table, param_copy)
        self.assertTrue(out.equals(out_table))

    def test_two_level_multi_op_multi_target(self):
        param_copy = defaultparams.copy()
        param_copy['groupby|groupby|0'] = "a"
        param_copy['groupby|groupby|1'] = "b"
        param_copy['active.addremove.last|groupby|1'] = True
        param_copy['operation|operation|0'] = 5
        param_copy['targetcolumn|operation|0'] = "c"
        param_copy['operation.show-sibling|operation|1'] = 4
        param_copy['targetcolumn.hide-with-sibling|operation|1'] = "e"
        param_copy['active.addremove|operation|1'] = True
        out_table = pd.DataFrame([
            ['bread', 'death', float(4), float(5)],
            ['bread', 'liberty', float(3), float(1)],
            ['roses', 'death', float('nan'), float(9)],
            ['roses', 'liberty', float(11), float(4)],
        ], columns=['a', 'b', 'Max of c', 'Min of e'])
        out = render(self.table, param_copy)
        self.assertTrue(out.equals(out_table))

    def test_two_level_same_op_multi_target(self):
        param_copy = defaultparams.copy()
        param_copy['groupby|groupby|0'] = "a"
        param_copy['groupby|groupby|1'] = "b"
        param_copy['active.addremove.last|groupby|1'] = True
        param_copy['operation|operation|0'] = 4
        param_copy['targetcolumn|operation|0'] = "c"
        param_copy['operation.show-sibling|operation|1'] = 4
        param_copy['targetcolumn.hide-with-sibling|operation|1'] = "e"
        param_copy['active.addremove|operation|1'] = True
        out_table = pd.DataFrame([
            ['bread', 'death', float(3), float(5)],
            ['bread', 'liberty', float(3), float(1)],
            ['roses', 'death', float('nan'), float(9)],
            ['roses', 'liberty', float(10), float(4)],
        ], columns=['a', 'b', 'Min of c', 'Min of e'])
        out = render(self.table, param_copy)
        self.assertTrue(out.equals(out_table))

    def test_custom_name(self):
        param_copy = defaultparams.copy()
        param_copy['groupby|groupby|0'] = "a"
        param_copy['outputname|operation|0'] = "Test"
        out_table = pd.DataFrame([
            ['bread', 4],
            ['roses', 3]
        ], columns=['a', 'Test'])
        out = render(self.table, param_copy)
        self.assertTrue(out.equals(out_table))

    def test_non_numeric_column_error(self):
        param_copy = defaultparams.copy()
        param_copy['groupby|groupby|0'] = "a"
        param_copy['operation|operation|0'] = 4
        param_copy['targetcolumn|operation|0'] = "date"
        out = render(self.table, param_copy)
        self.assertTrue(out == "Can't get min of non-numeric column 'date'")


if __name__ == '__main__':
    unittest.main()
