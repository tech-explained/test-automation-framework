# db/seeds/

The framework itself ships with **no seed data**. A bare install gives you
empty `tf.*` tables — you bring your own pipeline, register an environment
row pointing at your adapter, and author test cases with
`test_framework/add_case.py`.

Sample seed data lives in `examples/sample_seeds/`: example
environments, a sample pipeline row, suites, and illustrative test cases.
Adapt them to your own pipeline's tables before using.
