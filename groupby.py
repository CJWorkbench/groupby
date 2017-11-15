def render(wf_module, table):
    # obtain parameters
    groupby = wf_module.get_param_column('groupby')
    operation = wf_module.get_param_menu_idx("operation")
    targetcolumn = wf_module.get_param_column('targetcolumn')

    # while the user has not provided all the parameters, returns full table.
    if groupby == '' or (targetcolumn == '' and operation != 0):  # process without target column if counting
        wf_module.set_ready(notify=False)
        return table
    else:
        # if target column is not a numeric type, tries to convert it (before any aggregation)
        if targetcolumn != '' and (table[targetcolumn].dtype != np.float64 and table[targetcolumn].dtype != np.int64):
            table[targetcolumn] = table[targetcolumn].str.replace(',', '')
            table[targetcolumn] = table[targetcolumn].astype(float)

        if operation == 0: # count
            newtab = table.groupby([groupby])[[groupby]].count()
            newtab.columns = ['count'] # otherwise index name and count column name are the same, error on reset_index below
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
