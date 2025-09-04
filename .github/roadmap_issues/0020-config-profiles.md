## Summary

Add Pydantic v2 config models for connectors and catalog operations (profiles), to standardize validation and defaults.

## Motivation

Users need a simple, validated way to configure connections and locations across environments (local, Databricks, Fabric).

## Scope

- Define `BaseProfile` with common fields; add profiles per connector/catalog where useful.
- Support env-var interpolation and minimal secret handling via `os.environ`.

## Acceptance criteria

- [ ] Pydantic models in `spark_fuse.config` (new module)
- [ ] Unit tests for happy-path and validation errors
- [ ] Docs page with examples (YAML + Python)
