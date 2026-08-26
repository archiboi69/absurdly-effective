// Node/Testcontainers fixture for the production Absurd driver tests.
// oxlint-disable effecttsgo/async-function
// oxlint-disable effecttsgo/global-console
// oxlint-disable effecttsgo/global-random
// oxlint-disable effecttsgo/node-builtin-import

import { afterAll, beforeAll } from "@effect/vitest";
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import { readFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { Pool } from "pg";

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
  });
  await pool.query(readFileSync(join(currentDirectory, "../../../sql/absurd.sql"), "utf-8"));
});

afterAll(async () => {
  await pool?.end();
  await container?.stop();
});

export const randomName = (prefix: string): string =>
  `${prefix}_${Math.random().toString(36).slice(2)}`;
