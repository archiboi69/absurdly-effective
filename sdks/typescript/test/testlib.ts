export {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  assert,
  describe,
  expect,
  it,
  test,
  vi,
} from "@effect/vitest";

export const testLog = {
  log: (...args: unknown[]) => console.log(...args),
  info: (...args: unknown[]) => console.info(...args),
  warn: (...args: unknown[]) => console.warn(...args),
  error: (...args: unknown[]) => console.error(...args),
};
