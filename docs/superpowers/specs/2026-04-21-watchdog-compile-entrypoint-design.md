# `watchdog compile` CLI Entrypoint

**Status:** Approved
**Date:** 2026-04-21

---

## Problem

The compile-down framework emits runtime artifacts from policies, but there is no CLI entrypoint to run it. Today you'd have to call `compile_policies()` + `write_artifacts()` + `write_manifest()` manually in Python. A `compile` entrypoint completes the operator workflow: compile artifacts, then evaluate picks them up automatically.

## Goal

A `compile()` entrypoint in `entrypoints.py` that loads policies, compiles them, writes artifacts + manifest to `compile_output/`, runs drift detection, and prints a terse summary. Registered as `watchdog-compile` in `engine/setup.py`.

## Non-Goals

- **Deploy step.** Compile writes to disk. Deploying artifacts to the workspace is a separate feature.
- **Verbose per-artifact output.** The manifest has full details. The entrypoint prints a summary only.
- **New CLI flags beyond `--catalog` and `--schema`.** Matches existing entrypoint patterns.

---

## CLI Interface

```
watchdog-compile --catalog <catalog> --schema <schema>
```

Same argument pattern as `crawl`, `evaluate`, `notify`.

## Pipeline

1. Parse `--catalog` and `--schema` args
2. Create `SparkSession` (needed for `load_delta_policies`)
3. Load policies: `load_yaml_policies()` + `load_delta_policies(spark, catalog, schema)`
4. Filter to policies with `compile_to` (for reporting — `compile_policies` already skips others)
5. `compile_policies(policies)` → `list[EmittedArtifact]`
6. Resolve output directory: `compile_output/` relative to engine root (same `Path(__file__)` / `NameError` serverless pattern)
7. `write_artifacts(artifacts, output_dir)`
8. `write_manifest(artifacts, output_dir / "manifest.json")`
9. `check_drift(output_dir / "manifest.json", output_dir)` → `list[DriftResult]`
10. Print summary

## Output Location

`compile_output/` relative to the engine package root, using the same `Path(__file__).parent.parent.parent` / `NameError` fallback to `Path(os.getcwd())` pattern used for ontology detection and compile-down path detection in `_build_engine()`.

This is the directory `_build_engine()` already checks for `compile_output/manifest.json`. Zero-config pipeline: `watchdog-compile` writes here, `watchdog-evaluate` reads from here.

## Output Format

Terse summary matching the style of other entrypoints:

```
Compiled 5 policies → 8 artifacts (3 guardrails, 3 uc_tag_policy, 2 uc_abac). Drift: 8 in_sync, 0 drifted, 0 missing.
```

If no policies have `compile_to`:

```
No policies with compile_to found. Nothing to compile.
```

## Changes

| File | Change |
|------|--------|
| `engine/src/watchdog/entrypoints.py` | Add `compile()` function |
| `engine/setup.py` | Add `watchdog-compile=watchdog.entrypoints:compile` to `console_scripts` |

## Tests

The `compile()` entrypoint wires together already-tested functions (`compile_policies`, `write_artifacts`, `write_manifest`, `check_drift`). Since it requires `SparkSession` for `load_delta_policies`, a full unit test is not practical without mocking Spark.

A targeted unit test can verify the summary formatting logic if extracted to a helper. The core compile pipeline is already covered by 43 tests in `test_compiler.py`.

| Test | Asserts |
|------|---------|
| `test_format_compile_summary` | Correct artifact count grouping by target |
| `test_format_compile_summary_empty` | "Nothing to compile" message when no artifacts |
| `test_format_compile_summary_with_drift` | Correct drift state counts |
