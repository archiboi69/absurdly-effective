import { defineConfig } from "vite-plus";
import solid from "vite-plugin-solid";
import tailwindcss from "@tailwindcss/vite";
import path from "path";
import { consoleForwardPlugin } from "vite-console-forward-plugin";

export default defineConfig(({ mode }) => ({
  fmt: {},
  lint: {
    jsPlugins: [{ name: "vite-plus", specifier: "vite-plus/oxlint-plugin" }],
    rules: { "vite-plus/prefer-vite-plus-imports": "error" },
    options: { typeAware: true, typeCheck: true },
  },
  base: mode === "production" ? "/_static/" : "/",
  plugins: [solid(), tailwindcss(), mode === "production" ? null : consoleForwardPlugin()].filter(
    Boolean,
  ),
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  server: {
    host: "127.0.0.1",
    port: 7891,
    proxy: {
      "/api": {
        target: "http://127.0.0.1:7890",
        changeOrigin: true,
      },
      "/_static": {
        target: "http://127.0.0.1:7890",
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: "../internal/web/dist",
    emptyOutDir: true,
    rollupOptions: {
      output: {
        entryFileNames: "assets/index.js",
        chunkFileNames: "assets/[name].js",
        assetFileNames: "assets/[name][extname]",
      },
    },
  },
}));
