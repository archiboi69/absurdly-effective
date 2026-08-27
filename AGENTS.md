# Agents

This repository is the development and conformance workspace for
`absurdly-effective`, the Effect-native SDK built on Absurd. The published
product is `sdks/effect`; the rest of the Absurd fork supplies the stock SQL
contract, reference SDKs, documentation, and operational tools it integrates
with.

**Key concept**: A _task_ is subdivided into _steps_ that act as checkpoints.
Tasks can suspend (for sleep or events) and resume without data loss. All state
lives in Postgres tables prefixed by queue name (`t_`, `r_`, `c_`, `e_`, `w_`).

## Repository Structure

- **sdks/effect/** - Published `absurdly-effective` package and its tests.
- **sql/absurd.sql** - Stock Absurd persistence contract used by integration tests.
- **sdks/typescript/** - Upstream TypeScript SDK reference; do not modify for Effect interoperability.
- **sdks/python/** - Upstream Python SDK reference; do not modify for Effect interoperability.
- **habitat/**, **absurdctl**, **tests/** - Upstream operational and SQL-conformance tooling.

## Repository operation

Vite+ (`vp`) is the canonical package manager, formatter, linter, test runner,
and task runner for `sdks/effect`. Root Vite+ and Make targets intentionally
exclude the upstream TypeScript, Python, Habitat, and core test trees.

- Tooling: use `vp fmt`, `vp lint`, and `vp test`; do not add parallel tooling
  when Vite+ covers the job.
- Packages: use `vp install`; use `vp pm <command>` only when direct pnpm access
  is unavoidable. Do not run `pnpm` directly.
- Help: `vp help`, `vp <command> --help`, and `node_modules/vite-plus/docs`.

## Building and Testing

```bash
vp install
make format
make check
make test
make build
make pack
```

Effect tests import `@effect/vitest` directly; package-local test configuration
lives in `sdks/effect/vite.config.ts`.

Effect type-aware diagnostics run inside `vp lint`: the root `postinstall` hook
(`effect-tsgo patch --no-typescript --oxlint`) patches the Oxlint embedded in
Vite+ with the `effecttsgo` plugin, and `vite.config.ts` enables every
`effecttsgo` rule at `error` severity for `sdks/effect/**` via `lint.overrides`.
The `@effect/tsgo` version must stay
compatible with the Oxlint/`oxlint-tsgolint` versions bundled by Vite+ per the
[compatibility matrix](https://github.com/Effect-TS/tsgo#supported-package-versions).

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

```bash
git subtree pull \
    --prefix=.context/effect \
    https://github.com/Effect-TS/effect.git \
    main \
    --squash
```
