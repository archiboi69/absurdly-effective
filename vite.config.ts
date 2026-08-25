import { defineConfig } from "vite-plus";

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
  },
});
