# db/seeds/

The framework itself ships with **no seed data**. A bare install gives you
empty `tf.*` tables — you bring your own pipeline, register an environment
row pointing at your adapter, and author test cases with
`test_framework/add_case.py`.

Sample seed data lives with the reference example:

- `examples/reference_pipeline/db/seeds/` — environments (local → example
  adapter), the reference pipeline row, suites, and the 23-case HR QA
  matrix. Applied by `examples/reference_pipeline/db/apply.sh` after the
  framework migrations.
