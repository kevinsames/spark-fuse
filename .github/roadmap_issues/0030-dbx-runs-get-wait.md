## Summary

Databricks job runs: add `runs/get`, `runs/get-output`, and `--wait` support for `databricks-submit`.

## Acceptance criteria

- [ ] Helper(s) to call Runs APIs with error handling
- [ ] `--wait` flag polls run state and exits non-zero on failure
- [ ] Pretty status output in CLI
- [ ] Unit tests with mocked HTTP
