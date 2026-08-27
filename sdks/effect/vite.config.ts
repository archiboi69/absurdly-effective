import { defineConfig } from "vite-plus";

export default defineConfig({
  test: {
    fileParallelism: false,
    isolate: true,
    fakeTimers: {
      toFake: ["Date"],
    },
    silent: "passed-only",
    hookTimeout: 120_000,
    testTimeout: 60_000,
  },
});
