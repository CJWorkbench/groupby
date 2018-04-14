def render(table, params):
    import numpy as np
    param_obj = {}
    for k, v in params.items():
        name_parts = k.split('|')
        name_parts[0] = name_parts[0].split('.')[0]

        if len(name_parts) > 1:
            try:
                param_obj[name_parts[1]]
            except KeyError:
                param_obj[name_parts[1]] = {}

            try:
                param_obj[name_parts[1]][name_parts[2]]
            except:
                param_obj[name_parts[1]][name_parts[2]] = {}

            param_obj[name_parts[1]][name_parts[2]][name_parts[0]] = v

    for k, v in param_obj.items():
        # Turn a dict with numeric keys into an array
        param_obj[k] = [s[1] for s in sorted(v.items(), key=lambda x: x[0])]

    # obtain parameters
    # For every paramater in group 'groupby', return the 'groupby' property if it's not blank,
    # property 'active' is set to true, or if there is no property called 'active'.
    groupby_vals = [param['groupby'] for param in param_obj['groupby'] if param['groupby'] is not ''
                    and param.get('active', True)]

    if len(groupby_vals) == 0:
        return table
    else:
        grouped_table = table.groupby(groupby_vals)
        columns = [[val, val] for val in groupby_vals]
        agg_dict = {}
        op_names = [
            ('size', 'Count'),
            ('mean', 'Average'),
            ('sum', 'Sum'),
            ('min', 'Min'),
            ('max', 'Max'),
            ('nunique', 'Count unique')
        ]
        for op in param_obj['operation']:
            operation, label = op_names[int(op['operation'])]
            targetcolumn = op.get('targetcolumn', '')
            outputname = op.get('outputname', None)
            active = op.get('active', True)

            # If the operation name is set but not the target column (except size),
            # skip this operation
            if active is False or (operation != 'size' and targetcolumn == ''):
                continue

            # if target column is not a numeric type, tries to convert it (before any aggregation)
            if targetcolumn != '' and operation != 'nunique' and (
                    table[targetcolumn].dtype != np.float64 and table[targetcolumn].dtype != np.int64):
                try:
                    table[targetcolumn] = table[targetcolumn].str.replace(',', '')
                    table[targetcolumn] = table[targetcolumn].astype(float)
                except Exception:
                    return "Can't get " + operation + " of non-numeric column '" + targetcolumn + "'"

            if operation is 'size':
                target_col_name = columns[0][0]
            else:
                target_col_name = targetcolumn

            # If the column name doesn't exist as a key in our aggregation dictionary, create it
            try:
                agg_dict[target_col_name]
            except KeyError:
                agg_dict[target_col_name] = []

            # Check if we're already doing this operation, no reason to do it twice
            if operation not in agg_dict[target_col_name]:
                agg_dict[target_col_name].append(operation)
                colstr = '%s||%s' % (target_col_name, operation)
                columns.append([colstr, outputname, label])

        if agg_dict == {}:
            return table

        newtab = grouped_table.agg(agg_dict)
        newtab = newtab.reset_index()
        # Create column names in the shape 'columnname operation', unless the operation is size, in which case
        # just use 'count'
        newtab.columns = ['%s||%s' % (v[0], v[1]) if v[1] is not '' else v[0] for v in newtab.columns.values]
        newtab = newtab[[column[0] for column in columns]]

        sanitized_column_names = []

        for column in columns:
            if column[1] is not '':
                sanitized_column_names.append(column[1])
            else:
                name_parts = column[0].split('||')
                if len(name_parts) == 1:
                    sanitized_column_names.append(name_parts[0])
                elif name_parts[1] == 'size':
                    sanitized_column_names.append('Group Size')
                else:
                    sanitized_column_names.append('%s of %s' % (column[2], name_parts[0]))

        newtab.columns = sanitized_column_names
        return newtab
