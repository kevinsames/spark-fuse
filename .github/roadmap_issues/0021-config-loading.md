## Summary

Support loading config from YAML and environment, and selecting a profile via CLI flag (e.g., `--profile dev`).

## Motivation

Enable consistent configuration across local/dev/prod without code changes.

## Acceptance criteria

- [ ] YAML loader with env expansion (e.g., `${ENV_VAR}`)
- [ ] CLI `--profile` flag for relevant commands
- [ ] Docs with example `spark-fuse.yaml`
