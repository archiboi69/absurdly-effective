import {
  antipattern,
  correctness,
  effectNative,
  style,
} from "@effect/tsgo/oxlint-presets";
import { defineConfig } from "vite-plus";

// Every effecttsgo rule the @effect/tsgo presets ship, at error severity.
const effectRules = Object.fromEntries(
  [correctness, antipattern, effectNative, style].flatMap((preset) =>
    Object.keys(preset.rules ?? {}),
  ).map((rule): [string, "error"] => [rule, "error"]),
) as NonNullable<typeof correctness.rules>;

// Only the JS/TS workspace packages are linted/formatted; everything else
// (vendored references, Python, Go, docs) is out of scope for Vite+.
const outsideWorkspace = [
  ".context/**",
  ".github/**",
  ".pi/**",
  "docs/**",
  "habitat/cmd/**",
  "habitat/internal/**",
  "habitat/scripts/**",
  "scripts/**",
  "skills/**",
  "sql/**",
  "tests/**",
];

export default defineConfig({
  fmt: {
    ignorePatterns: [...outsideWorkspace, "*.md", "*.toml", "*.sql"],
  },
  lint: {
    jsPlugins: [{ name: "vite-plus", specifier: "vite-plus/oxlint-plugin" }],
    rules: { "vite-plus/prefer-vite-plus-imports": "error" },
    ignorePatterns: ["**/dist/**", "sdks/typescript/examples/**", ...outsideWorkspace],
    options: { typeAware: true, typeCheck: true },
    overrides: [
      {
        // Effect type-aware diagnostics via the effecttsgo plugin that
        // `@effect/tsgo`'s postinstall patch injects into the Oxlint binary.
        files: ["sdks/typescript/**"],
        plugins: ["effecttsgo"],
        rules: effectRules,
      },
    ],
  },
});
