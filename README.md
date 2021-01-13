groupby
-------

Workbench module that creates a row per group of input rows.

Developing
----------

First, get up and running:

`tox`

This will download dependencies and run tests. It should pass.

To add a feature on the Python side:

1. Write a test in `tests/`
2. Run `tox` to prove it breaks
3. Edit `groupby.py` to make the test pass
4. Run `tox` to prove it works
5. Commit and submit a pull request

To develop continuously on Workbench:

1. Check this code out in a sibling directory to your checked-out Workbench code
2. Start Workbench with `bin/dev start`
3. In a separate tab in the Workbench directory, run `bin/dev develop-module groupby`
4. Edit this code; the module will be reloaded in Workbench immediately
