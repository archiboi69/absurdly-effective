// This is the Node/Testcontainers fixture for the Effect-facing tests, not
// application Effect code. Hooks must satisfy Vitest's Promise contract and
// intentionally use Node filesystem, timers, randomness, and console logging.
// oxlint-disable effecttsgo/async-function
// oxlint-disable effecttsgo/global-console
// oxlint-disable effecttsgo/global-random
// oxlint-disable effecttsgo/global-timers
// oxlint-disable effecttsgo/new-promise
// oxlint-disable effecttsgo/node-builtin-import

import { Absurd, type JsonValue } from "absurd-sdk";
import { afterAll, beforeAll } from "@effect/vitest";
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import { readFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { Pool } from "pg";

const testLog = {
  log: (...args: unknown[]) => console.log(...args),
  info: (...args: unknown[]) => console.info(...args),
  warn: (...args: unknown[]) => console.warn(...args),
  error: (...args: unknown[]) => console.error(...args),
};

export interface TaskRow {
  task_id: string;
  task_name: string;
  params: JsonValue;
  headers: JsonValue | null;
  retry_strategy: JsonValue | null;
  max_attempts: number | null;
  cancellation: JsonValue | null;
  enqueue_at: Date;
  first_started_at: Date | null;
  state: "pending" | "running" | "sleeping" | "completed" | "failed" | "cancelled";
  attempts: number;
  last_attempt_run: string | null;
  completed_payload: JsonValue | null;
  cancelled_at: Date | null;
}

export interface RunRow {
  run_id: string;
  task_id: string;
  attempt: number;
  state: "pending" | "running" | "sleeping" | "completed" | "failed" | "cancelled";
  claimed_by: string | null;
  claim_expires_at: Date | null;
  available_at: Date;
  wake_event: string | null;
  event_payload: JsonValue | null;
  started_at: Date | null;
  completed_at: Date | null;
  failed_at: Date | null;
  result: JsonValue | null;
  failure_reason: JsonValue | null;
  created_at: Date;
}

export let container: StartedPostgreSqlContainer;
export let pool: Pool;

const currentDirectory = dirname(fileURLToPath(import.meta.url));

beforeAll(async () => {
  container = await new PostgreSqlContainer("postgres:16-alpine").withExposedPorts(5432).start();
  pool = new Pool({
    host: container.getHost(),
    port: container.getPort(),
    database: container.getDatabase(),
    user: container.getUsername(),
    password: container.getPassword(),
    max: 1,
  });

  const schemaPath = join(currentDirectory, "../../../sql/absurd.sql");
  await pool.query(readFileSync(schemaPath, "utf-8"));
});

afterAll(async () => {
  await pool?.end();
  await container?.stop();
});

export interface TestContext {
  absurd: Absurd;
  pool: typeof pool;
  queueName: string;
  cleanupTasks(): Promise<void>;
  getTask(taskID: string): Promise<TaskRow | null>;
  getRun(runID: string): Promise<RunRow | null>;
  getRuns(taskID: string): Promise<RunRow[]>;
  setFakeNow(ts: Date | null): Promise<void>;
  sleep(ms: number): Promise<void>;
}

export function randomName(prefix = "test"): string {
  return `${prefix}_${Math.random().toString(36).substring(7)}`;
}

export async function createTestAbsurd(queueName: string = "default"): Promise<TestContext> {
  const absurd = new Absurd({ db: pool, queueName, log: testLog });
  await absurd.createQueue(queueName);

  return {
    absurd,
    pool,
    queueName,
    cleanupTasks: () => cleanupTasks(queueName),
    getTask: (taskID: string) => getTask(taskID, queueName),
    getRun: (runID: string) => getRun(runID, queueName),
    getRuns: (taskID: string) => getRuns(taskID, queueName),
    setFakeNow,
    sleep: (ms: number) => new Promise((resolve) => setTimeout(resolve, ms)),
  };
}

async function setFakeNow(ts: Date | null): Promise<void> {
  if (ts === null) {
    await pool.query("SET absurd.fake_now = DEFAULT");
  } else {
    await pool.query("SELECT set_config('absurd.fake_now', $1, false)", [ts.toISOString()]);
  }
}

async function cleanupTasks(queue: string): Promise<void> {
  try {
    await pool.query(
      `TRUNCATE absurd.t_${queue}, absurd.r_${queue}, absurd.c_${queue}, absurd.e_${queue}, absurd.w_${queue}`,
    );
  } catch (error) {
    if (!(error instanceof Error) || !error.message.includes("does not exist")) {
      throw error;
    }
  }
}

async function getTask(taskID: string, queue: string): Promise<TaskRow | null> {
  const { rows } = await pool.query<TaskRow>(`SELECT * FROM absurd.t_${queue} WHERE task_id = $1`, [
    taskID,
  ]);
  return rows[0] ?? null;
}

async function getRun(runID: string, queue: string): Promise<RunRow | null> {
  const { rows } = await pool.query<RunRow>(`SELECT * FROM absurd.r_${queue} WHERE run_id = $1`, [
    runID,
  ]);
  return rows[0] ?? null;
}

async function getRuns(taskID: string, queue: string): Promise<RunRow[]> {
  const { rows } = await pool.query<RunRow>(
    `SELECT * FROM absurd.r_${queue} WHERE task_id = $1 ORDER BY attempt`,
    [taskID],
  );
  return rows;
}
