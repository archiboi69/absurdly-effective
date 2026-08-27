import { antipattern, correctness, effectNative, style } from "@effect/tsgo/oxlint-presets";
import { defineConfig } from "vite-plus";
import { configDefaults } from "vite-plus/test/config";

// Every effecttsgo rule the @effect/tsgo presets ship, at error severity.
// SAFETY: every preset exposes the same `rules` record shape, so merging their
// keys yields entries valid for `correctness.rules`.
const effectRules = Object.fromEntries(
  [correctness, antipattern, effectNative, style]
    .flatMap((preset) => Object.keys(preset.rules ?? {}))
    .map((rule): [string, "error"] => [rule, "error"]),
) as NonNullable<typeof correctness.rules>;

// Only the Effect product and this workspace's own tooling are linted and
// formatted. The rest of the Absurd fork stays available as upstream source
// and conformance infrastructure, not as code maintained by this workspace.
const outsideWorkspace = [
  ".context/**",
  ".github/**",
  ".pi/**",
  "docs/**",
  "habitat/**",
  "scripts/**",
  "sdks/python/**",
  "sdks/typescript/**",
  "skills/**",
  "sql/**",
  "tests/**",
];

const agentTooling = [
  ".agent/**",
  ".agents/**",
  ".claude/**",
  ".codex/**",
  ".continue/**",
  ".cursor/**",
  ".gemini/**",
  ".opencode/**",
  ".pi/**",
  ".roo/**",
  ".windsurf/**",
  ".zed/**",
  "tools/oxlint/anti-slop/**",
];

const antiSlopRuleNames = [
  "anti-slop/no-chained-type-assertions",
  "anti-slop/no-conditional-empty-object-spread",
  "anti-slop/no-known-value-widening",
  "anti-slop/no-module-mocking",
  "anti-slop/no-object-parameters",
  "anti-slop/no-reflect-apply",
  "anti-slop/no-reflect-get",
  "anti-slop/no-runtime-typeof",
  "anti-slop/no-shape-in-symbol-names",
  "anti-slop/no-unknown-parameters",
  "anti-slop/no-unknown-returns",
  "anti-slop/no-unknown-type-aliases",
  "anti-slop/no-unsafe-dictionary-type",
  "anti-slop/no-widen-then-assert",
  "anti-slop/require-safety-comment-for-type-assertion",
  "anti-slop-effect/no-service-constructor-imports",
];

export default defineConfig({
  test: {
    exclude: [...configDefaults.exclude, ".context/**"],
    hookTimeout: 120_000,
  },
  fmt: {
    ignorePatterns: [...outsideWorkspace, ...agentTooling, "*.md", "*.toml", "*.sql"],
  },
  lint: {
    jsPlugins: [
      { name: "vite-plus", specifier: "vite-plus/oxlint-plugin" },
      { name: "anti-slop", specifier: "./tools/oxlint/anti-slop/index.ts" },
      { name: "anti-slop-effect", specifier: "./tools/oxlint/anti-slop/effect/index.ts" },
    ],
    rules: {
      "vite-plus/prefer-vite-plus-imports": "error",
      ...Object.fromEntries(antiSlopRuleNames.map((rule): [string, "error"] => [rule, "error"])),
    },
    ignorePatterns: [
      "**/dist/**",
      ...outsideWorkspace,
      ...agentTooling,
    ],
    options: { typeAware: true, typeCheck: true },
    overrides: [
      {
        // Effect type-aware diagnostics via the effecttsgo plugin that
        // `@effect/tsgo`'s postinstall patch injects into the Oxlint binary.
        files: ["sdks/effect/**"],
        plugins: ["effecttsgo"],
        rules: effectRules,
      },
    ],
  },
});
