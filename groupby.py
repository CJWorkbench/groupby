import numpy as np

class Importable:
    @staticmethod
    def __init__(self):
        pass

    @staticmethod
    def event():
        pass

    @staticmethod
    def render(wf_module, table):
        # obtain parameters
        groupby = wf_module.get_param_column('groupby')
        operation = wf_module.get_param_menu_idx("operation")
        targetcolumn = wf_module.get_param_column('targetcolumn')

        # while the user has not provided all the parameters, returns full table.
        if targetcolumn == '':
            wf_module.set_ready(notify=False)
            return table
        else:
            # if target column is not a numeric type, tries to convert it (before any aggregation)
            if table[targetcolumn].dtype != np.float64 and table[targetcolumn].dtype != np.int64:
                table[targetcolumn] = table[targetcolumn].str.replace(',', '')
                table[targetcolumn] = table[targetcolumn].astype(float)
            # if operation must be performed on the entire table...
            if groupby == '':
                if operation == 0: # count
                    newtab = table[[targetcolumn]].count().to_frame()
                elif operation == 1: # average
                    newtab = table[[targetcolumn]].mean().to_frame()
                elif operation == 2: # sum
                    newtab = table[[targetcolumn]].sum().to_frame()
                elif operation == 3: # min
                    newtab = table[[targetcolumn]].min().to_frame()
                elif operation == 4: # max
                    newtab = table[[targetcolumn]].max().to_frame()
                newtab.columns = [targetcolumn]
            else:
                if operation == 0: # count
                    newtab = table.groupby([groupby])[[targetcolumn]].count()
                elif operation == 1: # average
                    newtab = table.groupby([groupby])[[targetcolumn]].mean()
                elif operation == 2: # sum
                    newtab = table.groupby([groupby])[[targetcolumn]].sum()
                elif operation == 3: # min
                    newtab = table.groupby([groupby])[[targetcolumn]].min()
                elif operation == 4: # max
                    newtab = table.groupby([groupby])[[targetcolumn]].max()
                newtab.reset_index(level=0, inplace=True)
            wf_module.set_ready(notify=False)
            return newtab