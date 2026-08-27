# Agents

Absurd is a Postgres-native durable workflow system. It moves the complexity of
durable execution into the database layer via stored procedures, keeping SDKs
lightweight and language-agnostic. The core principle is to handle tasks that
may run for minutes, days, or years without losing state.

**Key concept**: A _task_ is subdivided into _steps_ that act as checkpoints.
Tasks can suspend (for sleep or events) and resume without data loss. All state
lives in Postgres tables prefixed by queue name (`t_`, `r_`, `c_`, `e_`, `w_`).

## Repository Structure

- **sql/absurd.sql** - Core Postgres schema and stored procedures. This is the heart of the system.
- **sdks/typescript/** - TypeScript SDK (`absurd-sdk` on npm)
- **sdks/effect/** - Effect-native tasks and optional `WorkflowEngine` adapter (`absurd-effect` on npm)
- **habitat/** - Go-based web UI for monitoring tasks, runs, and events
- **absurdctl** - Python-based CLI tool for queue/task management and debugging
- **tests/** - Python test suite using psycopg and testcontainers

## Repository operation

Vite+ (`vp`) is the canonical runtime, package manager, task runner, formatter,
linter, test runner, bundler, and Vite frontend interface.

- Tooling: use `vp fmt`, `vp lint`, and `vp test`; do not add parallel tooling
  when Vite+ covers the job.
- Packages: use `vp install`; use `vp pm <command>` only when direct pnpm access
  is unavoidable. Do not run `pnpm` directly.
- Help: `vp help`, `vp <command> --help`, and `node_modules/vite-plus/docs`.

## Building and Testing

### TypeScript SDK

```bash
vp install              # from the workspace root
vp run absurd-sdk#build # Compiles to dist/ (both ESM and CommonJS)
vp test                 # Run SDK tests (vitest via Vite+)
```

Tests use vitest through Vite+ (`vp test run` / `vp test watch` from
`sdks/typescript`); shared config lives in `sdks/typescript/vite.config.ts`
under the `test` section — there is no separate `vitest.config.ts`.

### Effect SDK

```bash
vp install                 # from the workspace root
vp run absurd-effect#build # Compiles to dist/ (both ESM and CommonJS)
cd sdks/effect && vp test run
```

Effect tests import `@effect/vitest` directly; package-local test configuration
lives in `sdks/effect/vite.config.ts`.

Effect type-aware diagnostics run inside `vp lint`: a root `postinstall` hook
(`effect-tsgo patch --no-typescript --oxlint`) patches the Oxlint embedded in
Vite+ with the `effecttsgo` plugin, and `vite.config.ts` enables every
`effecttsgo` rule at `error` severity for `sdks/effect/**` and
`sdks/typescript/**` via
`lint.overrides`. The `@effect/tsgo` version must stay
compatible with the Oxlint/`oxlint-tsgolint` versions bundled by Vite+ per the
[compatibility matrix](https://github.com/Effect-TS/tsgo#supported-package-versions).

### Habitat (Web UI)

```bash
cd habitat
make build      # Build UI + Go binary -> bin/habitat
make dev        # Run dev server with hot reload
./bin/habitat run -db-name your-database-name
```

### Python Tests

```bash
cd tests
uv run pytest              # Run all tests
uv run pytest test_foo.py  # Run single test file
```

### Formatting

```bash
make format     # Format TypeScript/Effect SDKs, habitat UI, and Python tests
```

## Habitat Web UI

Habitat connects directly to Postgres and serves a SolidJS dashboard. It shows
task state, run history, checkpoint data, and events. Useful for debugging and
monitoring.

Configuration via flags or `HABITAT_*` env vars (see habitat/README.md).

## Additional Information

More information can be found in @README.md and @CONTRIBUTING.md

## References

Reference codebases are vendored under `.context/` as squashed git subtrees.

- Use vendored repositories as read-only reference material.
- Prefer examples and patterns from vendored source over generated guesses or web search.
- Do not edit files under `.context/` unless explicitly asked.
- Do not import from `.context/`; application code must import normal package dependencies.

### Effect

The Effect v4 repository is vendored at `.context/effect` for reference. Use it to explore APIs, find usage examples, and understand implementation details when the documentation is not enough. Before writing Effect code, inspect `.context/effect/LLMS.md` if present and update the Effect source if the worktree is clean, run from the workspace root:

Use `vp run check:effect` to surface Effect language-service diagnostics that `vp check` does not report.

```bash
git subtree pull \
    --prefix=.context/effect \
    https://github.com/Effect-TS/effect.git \
    main \
    --squash
```
