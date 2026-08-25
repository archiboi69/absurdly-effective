/**
 * Idiomatic Effect v4 `WorkflowEngine` adapter backed by Absurd queues.
 *
 * `AbsurdWorkflowEngine.inQueue` annotates an Effect `Workflow` with the
 * physical Absurd queue it runs on, and `AbsurdWorkflowEngine.layer` provides
 * `WorkflowEngine.WorkflowEngine` from a generic `effect/unstable/sql`
 * `SqlClient`. Definitions, execution, polling, handler registration,
 * activities, and durable deferreds remain Effect-owned; Absurd persists the
 * tasks, checkpoints, events, and waits that make them durable.
 *
 * Workflow execution identity is Effect's deterministic execution ID, mapped
 * to Absurd's idempotency key. Absurd UUID task/run IDs stay internal.
 *
 * Infrastructure failures surface as defects, matching Effect `WorkflowEngine`
 * semantics; workflow typed failures are preserved through encoded results.
 *
 * Requires PostgreSQL: statements target the `absurd` schema functions and
 * queue-prefixed tables with `$n` placeholders.
 */
// This module implements Effect's low-level `WorkflowEngine.Encoded`
// contract, whose public surface intentionally passes encoded payloads and
// results as untyped values (the same shape as upstream's own engine
// implementations, which are lint-excluded). These boundary rules are
// disabled for the adapter as a whole rather than scattering suppressions
// through every contract-mandated signature.
// oxlint-disable anti-slop/no-object-parameters
// oxlint-disable anti-slop/no-unknown-parameters
// oxlint-disable effecttsgo/any-unknown-in-error-context
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FiberMap from "effect/FiberMap";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import { SqlClient, SqlError } from "effect/unstable/sql";
import {
  type Activity,
  type DurableDeferred,
  Workflow,
  WorkflowEngine,
} from "effect/unstable/workflow";
import * as os from "os";

/**
 * Annotation key holding the Absurd queue a workflow is bound to via
 * `AbsurdWorkflowEngine.inQueue`.
 */
class QueueAnnotation extends Context.Reference<string>(
  "absurd-sdk/AbsurdWorkflowEngine/QueueAnnotation",
  { defaultValue: () => "" },
) {}

const MAX_QUEUE_NAME_LENGTH = 57;
const CLAIM_TIMEOUT_SECONDS = 120;
const UNKNOWN_TASK_DEFER_BASE_SECONDS = 15;
const UNKNOWN_TASK_DEFER_JITTER_SECONDS = 15;
const SUSPEND_RETRY_SECONDS = 0.25;

interface QueueConfig {
  readonly name: string;
  readonly concurrency: number;
  readonly pollIntervalMillis: number;
}

export interface QueueOptions {
  /**
   * Maximum workflow executions processed in parallel for this queue.
   *
   * @default 1
   */
  readonly concurrency?: number | undefined;
  /**
   * Idle delay between claim polls.
   *
   * @default 250ms
   */
  readonly pollInterval?: Duration.Input | undefined;
}

export interface LayerOptions {
  readonly queues: Readonly<Record<string, QueueOptions>>;
}

interface Registration {
  readonly workflow: Workflow.Any;
  readonly queue: string;
  readonly execute: (
    payload: object,
    executionId: string,
  ) => Effect.Effect<
    unknown,
    unknown,
    WorkflowEngine.WorkflowInstance | WorkflowEngine.WorkflowEngine
  >;
}

interface ClaimedRow {
  run_id: string;
  task_id: string;
  attempt: number;
  task_name: string;
  params: unknown;
}

interface TaskInfo {
  readonly taskId: string;
  readonly lastAttemptRun: string | null;
}

interface TaskSnapshot {
  state: string;
  result: unknown;
  failure_reason: unknown;
}

interface AwaitEventRow {
  should_suspend: boolean;
  payload: unknown;
}

// Checkpoints and payloads persist plain JSON structures; these codecs
// convert between stored JSON and real Effect `Exit`/`Result` values.
const exitStoreCodec = Schema.toCodecJson(Schema.Exit(Schema.Any, Schema.Any, Schema.Defect()));

const encodeExitStored = Schema.encodeSync(exitStoreCodec);
const decodeExitStored = Schema.decodeUnknownSync(exitStoreCodec);

// SAFETY: Absurd's jsonb columns are the system of record for these values;
// encoding arbitrary encoded workflow data to JSON text is the whole job of
// this boundary, so `Schema.Unknown` is the accurate contract here.
const unknownToJsonText = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

/**
 * JSON drops `undefined`, so a void success must be persisted with an
 * explicit `null` value to survive the round trip.
 */
interface StoredExit {
  _tag: "Success" | "Failure";
  value?: unknown;
  cause?: unknown;
}

const storedExitForJson = (exit: Exit.Exit<unknown, unknown>): StoredExit => {
  // SAFETY: the JSON codec emits exactly `_tag` plus `value`/`cause`; its
  // declared encoding is only the generic JSON object bound.
  // oxlint-disable-next-line anti-slop/no-chained-type-assertions
  const stored = encodeExitStored(exit) as unknown as StoredExit;
  if (stored._tag === "Success" && !("value" in stored)) {
    stored.value = null;
  }
  return stored;
};

const exec = <A>(
  self: Effect.Effect<ReadonlyArray<A>, SqlError.SqlError>,
): Effect.Effect<ReadonlyArray<A>> => Effect.orDie(self);

const validateQueueName = (queueName: string): string => {
  if (queueName === "" || !/^[A-Za-z0-9_]+$/.test(queueName)) {
    throw new Error(`Invalid Absurd queue name "${queueName}": expected non-empty [A-Za-z0-9_].`);
  }
  if (queueName.length > MAX_QUEUE_NAME_LENGTH) {
    throw new Error(
      `Queue name "${queueName}" is too long (max ${MAX_QUEUE_NAME_LENGTH} characters).`,
    );
  }
  return queueName;
};

const activityCheckpointName = (name: string, attempt: number): string =>
  `$activity:${name}:${attempt}`;
const deferredCheckpointName = (name: string): string => `$defer:${name}`;
const deferredEventName = (
  workflowTag: string,
  executionId: string,
  deferredName: string,
): string => `absurd-effect:deferred:${workflowTag}:${executionId}:${deferredName}`;

const deterministicJitterSeconds = (seed: string, maxJitterSeconds: number): number => {
  let hash = 2166136261;
  for (let i = 0; i < seed.length; i += 1) {
    hash ^= seed.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return Math.abs(hash) % (maxJitterSeconds + 1);
};

const encodePayloadUnsafe = (workflow: Workflow.Any, payload: object): object => {
  const rawCodec: unknown = Schema.toCodecJson(workflow.payloadSchema);
  // SAFETY: `toCodecJson` yields a codec for the payload struct whose encoded
  // side is JSON-safe; only the object-level encode direction is used here.
  const codec = rawCodec as Schema.Codec<object>;
  try {
    return Schema.encodeSync(codec)(payload);
  } catch (cause) {
    throw new Error(`Failed to encode workflow payload for "${workflow._tag}"`, {
      cause,
    });
  }
};

const decodePayloadUnsafe = (
  taskName: string,
  schema: Workflow.AnyStructSchema,
  params: unknown,
): object => {
  try {
    // SAFETY: struct payload schemas produce handler-facing record values.
    return schema.make(params) as object;
  } catch (cause) {
    throw new Error(`Failed to decode workflow payload for "${taskName}"`, { cause });
  }
};

/**
 * Provides an Effect-native durable `WorkflowEngine` backed by Absurd.
 */
export const AbsurdWorkflowEngine = {
  inQueue:
    (queueName: string) =>
    <W extends Workflow.Any>(workflow: W): W => {
      validateQueueName(queueName);
      // SAFETY: `Workflow.Any` omits `annotate`, but every runtime workflow
      // definition carries it; annotating returns a copy that preserves the
      // caller's precise workflow type W.
      // oxlint-disable-next-line anti-slop/no-chained-type-assertions
      return (workflow as unknown as Workflow.Workflow<string, never, never, never>).annotate(
        QueueAnnotation,
        queueName,
      ) as unknown as W;
    },

  layer: (
    options: LayerOptions,
  ): Layer.Layer<WorkflowEngine.WorkflowEngine, never, SqlClient.SqlClient> =>
    Layer.effect(
      WorkflowEngine.WorkflowEngine,
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;

        const queues: Array<QueueConfig> = Object.entries(options.queues).map(([name, config]) => ({
          name: validateQueueName(name),
          concurrency: Math.max(config.concurrency ?? 1, 1),
          pollIntervalMillis: Duration.toMillis(
            Duration.fromInputUnsafe(config.pollInterval ?? Duration.millis(250)),
          ),
        }));
        const workerId = `absurd-effect:${os.hostname?.() ?? "host"}:${process.pid}`;
        const registrations = new Map<string, Registration>();
        const clocks = yield* FiberMap.make<string>();

        const queueFor = (workflow: Workflow.Any): string => {
          const queue = Context.getOption(workflow.annotations, QueueAnnotation);
          if (Option.isNone(queue) || queue.value === "") {
            throw new Error(
              `Workflow "${workflow._tag}" has no Absurd queue annotation; wrap it with AbsurdWorkflowEngine.inQueue.`,
            );
          }
          return queue.value;
        };

        // ------------------------------------------------------------------
        // Absurd access layer
        // ------------------------------------------------------------------

        const requireTaskInfo = Effect.fnUntraced(function* (
          queue: string,
          executionId: string,
        ): Effect.fn.Return<TaskInfo> {
          const rows = yield* exec(
            sql.unsafe<{
              task_id: string;
              last_attempt_run: string | null;
            }>(
              `select task_id, last_attempt_run from absurd.t_${queue} where idempotency_key = $1`,
              [executionId],
            ),
          );
          const row = rows[0];
          if (row === undefined) {
            return yield* Effect.die(
              `No Absurd task found for execution "${executionId}" in queue "${queue}".`,
            );
          }
          return { taskId: row.task_id, lastAttemptRun: row.last_attempt_run };
        });

        const spawnTask = Effect.fnUntraced(function* (
          workflow: Workflow.Any,
          queue: string,
          executionId: string,
          payload: object,
        ): Effect.fn.Return<{ taskId: string }> {
          const rows = yield* exec(
            sql.unsafe<{
              task_id: string;
              run_id: string;
              attempt: number;
              created: boolean;
            }>(`select task_id, run_id, attempt, created from absurd.spawn_task($1, $2, $3, $4)`, [
              queue,
              workflow._tag,
              unknownToJsonText(payload),
              unknownToJsonText({
                idempotency_key: executionId,
                retry_strategy: { kind: "fixed", base_seconds: 1 },
              }),
            ]),
          );
          const row = rows[0];
          if (row === undefined) {
            return yield* Effect.die(`Failed to spawn Absurd task for "${workflow._tag}".`);
          }
          return { taskId: row.task_id };
        });

        const getCheckpoint = Effect.fnUntraced(function* (
          queue: string,
          taskId: string,
          checkpointName: string,
        ): Effect.fn.Return<unknown> {
          const rows = yield* exec(
            sql.unsafe<{ state: unknown }>(
              `select state from absurd.get_task_checkpoint_state($1, $2, $3)`,
              [queue, taskId, checkpointName],
            ),
          );
          return rows[0]?.state;
        });

        const setCheckpoint = Effect.fnUntraced(function* (
          queue: string,
          taskId: string,
          checkpointName: string,
          state: unknown,
          ownerRunId: string | null,
        ): Effect.fn.Return<void> {
          if (ownerRunId === null) {
            return yield* Effect.die(
              `Task "${taskId}" has no active run to own checkpoint "${checkpointName}".`,
            );
          }
          yield* exec(
            sql.unsafe(`select absurd.set_task_checkpoint_state($1, $2, $3, $4, $5)`, [
              queue,
              taskId,
              checkpointName,
              unknownToJsonText(state),
              ownerRunId,
            ]),
          );
        });

        const completeRun = Effect.fnUntraced(function* (
          queue: string,
          runId: string,
          result: unknown,
        ): Effect.fn.Return<void> {
          yield* exec(
            sql.unsafe(`select absurd.complete_run($1, $2, $3)`, [
              queue,
              runId,
              unknownToJsonText(result),
            ]),
          );
        });

        const scheduleRunInSeconds = Effect.fnUntraced(function* (
          queue: string,
          runId: string,
          seconds: number,
        ): Effect.fn.Return<void> {
          yield* exec(
            sql.unsafe(
              `select absurd.schedule_run($1, $2, absurd.current_time() + make_interval(secs => $3))`,
              [queue, runId, seconds],
            ),
          );
        });

        const awaitEvent = Effect.fnUntraced(function* (
          queue: string,
          taskId: string,
          runId: string,
          checkpointName: string,
          eventName: string,
        ): Effect.fn.Return<boolean> {
          const rows = yield* exec(
            sql.unsafe<AwaitEventRow>(
              `select should_suspend, payload from absurd.await_event($1, $2, $3, $4, $5)`,
              [queue, taskId, runId, checkpointName, eventName],
            ),
          );
          const row = rows[0];
          if (row === undefined) {
            return yield* Effect.die("absurd.await_event returned no row.");
          }
          return row.should_suspend;
        });

        const cancelTaskByExecutionId = Effect.fnUntraced(function* (
          workflow: Workflow.Any,
          executionId: string,
        ): Effect.fn.Return<void> {
          const queue = queueFor(workflow);
          const info = yield* requireTaskInfo(queue, executionId).pipe(Effect.option);
          if (Option.isNone(info)) return;
          yield* exec(sql.unsafe(`select absurd.cancel_task($1, $2)`, [queue, info.value.taskId]));
        });

        const wakeTaskByExecutionId = Effect.fnUntraced(function* (
          workflow: Workflow.Any,
          executionId: string,
        ): Effect.fn.Return<void> {
          const queue = queueFor(workflow);
          const info = yield* requireTaskInfo(queue, executionId).pipe(Effect.option);
          if (Option.isNone(info)) return;
          yield* exec(
            sql.unsafe(
              `
                update absurd.r_${queue} r
                   set available_at = absurd.current_time()
                  from absurd.t_${queue} t
                 where t.idempotency_key = $1
                   and r.run_id = t.last_attempt_run
                   and r.state = 'sleeping'
                   and t.state not in ('completed', 'failed', 'cancelled')
              `,
              [executionId],
            ),
          );
        });

        // ------------------------------------------------------------------
        // Worker loop
        // ------------------------------------------------------------------

        const deferUnknownRun = Effect.fnUntraced(function* (
          queue: string,
          runId: string,
        ): Effect.fn.Return<void> {
          const jitter = deterministicJitterSeconds(runId, UNKNOWN_TASK_DEFER_JITTER_SECONDS);
          yield* scheduleRunInSeconds(queue, runId, UNKNOWN_TASK_DEFER_BASE_SECONDS + jitter);
        });

        const heartbeatLoop = (queue: string, runId: string) =>
          sql
            .unsafe(`select absurd.extend_claim($1, $2, $3)`, [queue, runId, CLAIM_TIMEOUT_SECONDS])
            .pipe(
              Effect.delay(Duration.seconds(CLAIM_TIMEOUT_SECONDS / 2)),
              Effect.orDie,
              Effect.ignore,
              Effect.forever,
            );

        const parkSuspended = Effect.fnUntraced(function* (
          registration: Registration,
          claimed: ClaimedRow,
          instance: WorkflowEngine.WorkflowInstance["Service"],
        ): Effect.fn.Return<void> {
          const queue = registration.queue;
          let parked = false;
          for (const name of instance.awaitedDeferreds) {
            const checkpointName = deferredCheckpointName(name);
            const existing = yield* getCheckpoint(queue, claimed.task_id, checkpointName);
            if (existing !== undefined && existing !== null) continue;
            parked = yield* awaitEvent(
              queue,
              claimed.task_id,
              claimed.run_id,
              checkpointName,
              deferredEventName(registration.workflow._tag, instance.executionId, name),
            );
            if (parked) break;
          }
          if (!parked) {
            yield* scheduleRunInSeconds(queue, claimed.run_id, SUSPEND_RETRY_SECONDS);
          }
        });

        const processClaim = (
          engine: WorkflowEngine.WorkflowEngine["Service"],
          queue: string,
          claimed: ClaimedRow,
        ) =>
          Effect.gen(function* () {
            const registration = registrations.get(claimed.task_name);
            if (registration === undefined) {
              return yield* deferUnknownRun(queue, claimed.run_id);
            }

            const rows = yield* exec(
              sql.unsafe<{
                idempotency_key: string | null;
              }>(`select idempotency_key from absurd.t_${queue} where task_id = $1`, [
                claimed.task_id,
              ]),
            );
            const executionId = rows[0]?.idempotency_key;
            if (executionId === undefined || executionId === null) {
              return yield* deferUnknownRun(queue, claimed.run_id);
            }

            const payload = decodePayloadUnsafe(
              claimed.task_name,
              registration.workflow.payloadSchema,
              claimed.params,
            );

            // Extends the claim lease while the replay runs; closed with the
            // per-claim scope below.
            yield* Effect.forkScoped(heartbeatLoop(queue, claimed.run_id));

            const instance = WorkflowEngine.WorkflowInstance.initial(
              registration.workflow,
              executionId,
            );

            const result = yield* registration
              .execute(payload, executionId)
              .pipe(
                Effect.provideService(WorkflowEngine.WorkflowEngine, engine),
                Workflow.intoResult,
                Effect.provideService(WorkflowEngine.WorkflowInstance, instance),
              );

            if (result._tag === "Complete") {
              yield* completeRun(queue, claimed.run_id, {
                _tag: "Complete",
                exit: storedExitForJson(result.exit),
              });
            } else {
              yield* parkSuspended(registration, claimed, instance);
            }
          }).pipe(Effect.scoped);

        const workerLoop = (
          engine: WorkflowEngine.WorkflowEngine["Service"],
          config: QueueConfig,
        ) =>
          Effect.forever(
            Effect.gen(function* () {
              const claimed = yield* exec(
                sql.unsafe<ClaimedRow>(
                  `select run_id, task_id, attempt, task_name, params
                   from absurd.claim_task($1, $2, $3, $4)`,
                  [config.name, workerId, CLAIM_TIMEOUT_SECONDS, 1],
                ),
              );
              const task = claimed[0];
              if (task === undefined) {
                return yield* Effect.sleep(Duration.millis(config.pollIntervalMillis));
              }
              yield* processClaim(engine, config.name, task).pipe(
                Effect.catchCause((cause) =>
                  Effect.logError(
                    `[absurd-effect] workflow run ${task.task_name} (${task.task_id}/${task.run_id}) failed`,
                    cause,
                  ),
                ),
              );
            }),
          );

        // ------------------------------------------------------------------
        // Encoded engine implementation
        // ------------------------------------------------------------------

        const taskSnapshot = Effect.fnUntraced(function* (
          queue: string,
          taskId: string,
        ): Effect.fn.Return<TaskSnapshot | undefined> {
          const rows = yield* exec(
            sql.unsafe<TaskSnapshot>(
              `select state, result, failure_reason from absurd.get_task_result($1, $2)`,
              [queue, taskId],
            ),
          );
          return rows[0];
        });

        // Rehydrates engine-written results. Values are trusted here: they
        // were schema-encoded when the handler produced them and this engine
        // is their only writer, so no consumer-schema re-validation happens
        // (which would also couple decoding to foreign Schema instances).
        const readCompletedResult = Effect.fnUntraced(function* (
          workflow: Workflow.Any,
          snapshot: TaskSnapshot,
        ): Effect.fn.Return<Workflow.Result<unknown, unknown>> {
          // SAFETY: completed payloads are `{ _tag, exit }` records written
          // by this engine's worker on run completion.
          const stored = (snapshot.result ?? {}) as { _tag?: string; exit?: unknown };
          if (stored._tag !== "Complete" || stored.exit === undefined) {
            return yield* Effect.die(
              `Workflow "${workflow._tag}" completed without a stored result.`,
            );
          }
          return new Workflow.Complete({ exit: decodeExitStored(stored.exit) });
        });

        const completeDeferredInternal = Effect.fnUntraced(function* (
          queue: string,
          workflowTag: string,
          executionId: string,
          deferredName: string,
          exit: Exit.Exit<unknown, unknown>,
        ): Effect.fn.Return<void> {
          const storedExit = storedExitForJson(exit);
          const payloadText = unknownToJsonText(storedExit);
          const statement = sql.unsafe(`select absurd.emit_event($1, $2, $3)`, [
            queue,
            deferredEventName(workflowTag, executionId, deferredName),
            payloadText,
          ]);
          yield* exec(statement);
        });

        const engine = WorkflowEngine.makeUnsafe({
          register: (workflow, execute) =>
            Effect.sync(() => {
              registrations.set(workflow._tag, {
                workflow,
                execute,
                queue: queueFor(workflow),
              });
            }),

          execute: (workflow, opts) =>
            Effect.suspend(() => {
              const queue = queueFor(workflow);
              const spawn = Effect.map(
                spawnTask(
                  workflow,
                  queue,
                  opts.executionId,
                  encodePayloadUnsafe(workflow, opts.payload),
                ),
                (spawned) => spawned.taskId,
              );

              const awaitResult: Effect.Effect<Workflow.Result<unknown, unknown>> = Effect.flatMap(
                spawn,
                (taskId) => {
                  const pollSnapshot: Effect.Effect<Workflow.Result<unknown, unknown>> =
                    Effect.flatMap(taskSnapshot(queue, taskId), (snapshot) => {
                      if (snapshot === undefined) {
                        return Effect.die(
                          `Absurd task for execution "${opts.executionId}" vanished from queue "${queue}".`,
                        );
                      }
                      if (snapshot.state === "completed") {
                        return readCompletedResult(workflow, snapshot);
                      } else if (snapshot.state === "failed") {
                        return Effect.die(
                          new Error(
                            `Workflow "${workflow._tag}" run failed at the infrastructure level.`,
                            { cause: snapshot.failure_reason },
                          ),
                        );
                      } else if (snapshot.state === "cancelled") {
                        return Effect.die(
                          `Workflow "${workflow._tag}" execution "${opts.executionId}" was cancelled.`,
                        );
                      }
                      return Effect.succeed(Workflow.Suspended.make({}));
                    });
                  return pollSnapshot;
                },
              );

              const run = opts.discard ? Effect.asVoid(spawn) : awaitResult;
              // SAFETY: `Encoded.execute` resolves the `Discard` conditional
              // at its call site; each arm produces the value expected for
              // its instantiation.
              return run as Effect.Effect<
                typeof opts.discard extends true ? void : Workflow.Result<unknown, unknown>
              >;
            }),

          poll: Effect.fnUntraced(function* (
            workflow: Workflow.Any,
            executionId: string,
          ): Effect.fn.Return<Option.Option<Workflow.Result<unknown, unknown>>> {
            const queue = queueFor(workflow);
            const info = yield* requireTaskInfo(queue, executionId).pipe(Effect.option);
            if (Option.isNone(info)) return Option.none();
            const snapshot = yield* taskSnapshot(queue, info.value.taskId);
            if (snapshot === undefined) return Option.none();

            if (snapshot.state === "completed") {
              return Option.some(yield* readCompletedResult(workflow, snapshot));
            } else if (snapshot.state === "sleeping") {
              return Option.some(Workflow.Suspended.make({}));
            } else if (snapshot.state === "failed" || snapshot.state === "cancelled") {
              return yield* Effect.die(
                new Error(`Workflow "${workflow._tag}" ended in state "${snapshot.state}".`, {
                  cause: snapshot.failure_reason,
                }),
              );
            }
            return Option.none();
          }),

          interrupt: (workflow, executionId) => cancelTaskByExecutionId(workflow, executionId),

          // Cancellation is permanent (Absurd `cancel_task`); compensation
          // finalizers do not run across process boundaries.
          interruptUnsafe: (workflow, executionId) =>
            cancelTaskByExecutionId(workflow, executionId),

          resume: (workflow, executionId) => wakeTaskByExecutionId(workflow, executionId),

          activityExecute: Effect.fnUntraced(function* (activity: Activity.Any, attempt: number) {
            const instance = yield* WorkflowEngine.WorkflowInstance;
            const queue = queueFor(instance.workflow);
            const info = yield* requireTaskInfo(queue, instance.executionId);

            const checkpointName = activityCheckpointName(activity.name, attempt);
            const stored = yield* getCheckpoint(queue, info.taskId, checkpointName);
            if (stored !== undefined && stored !== null) {
              // SAFETY: activity checkpoints store `Workflow.Complete` JSON
              // written by this same engine.
              const record = stored as { _tag?: string; exit?: unknown };
              return new Workflow.Complete({ exit: decodeExitStored(record.exit) });
            }

            // SAFETY: `Activity.Any.executeEncoded` is typed with `any`
            // service/error channels; the provided instance and scoped
            // environment satisfy its real requirements.
            const encodedRun = activity.executeEncoded as Effect.Effect<
              unknown,
              unknown,
              Scope.Scope
            >;
            const exit = yield* encodedRun.pipe(
              Effect.provideService(
                WorkflowEngine.WorkflowInstance,
                WorkflowEngine.WorkflowInstance.initial(instance.workflow, instance.executionId),
              ),
              Effect.scoped,
              Effect.exit,
            );
            if (Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)) {
              // Never persist interrupts: shutdown must leave replay to the
              // next attempt instead of freezing an interrupted result.
              return yield* Effect.interrupt;
            }

            const current = yield* requireTaskInfo(queue, instance.executionId);
            yield* setCheckpoint(
              queue,
              current.taskId,
              checkpointName,
              { _tag: "Complete", exit: storedExitForJson(exit) },
              current.lastAttemptRun,
            );
            return new Workflow.Complete({ exit });
          }),

          deferredResult: Effect.fnUntraced(function* (deferred: DurableDeferred.Any) {
            const instance = yield* WorkflowEngine.WorkflowInstance;
            const queue = queueFor(instance.workflow);
            const info = yield* requireTaskInfo(queue, instance.executionId);
            const stored = yield* getCheckpoint(
              queue,
              info.taskId,
              deferredCheckpointName(deferred.name),
            );
            if (stored === undefined || stored === null) return Option.none();
            return Option.some(decodeExitStored(stored));
          }),

          deferredDone: (opts) =>
            Effect.suspend(() => {
              const registration = registrations.get(opts.workflowName);
              if (registration === undefined) {
                return Effect.die(
                  `Cannot complete deferred "${opts.deferredName}": workflow "${opts.workflowName}" is not registered on this engine.`,
                );
              }
              return completeDeferredInternal(
                registration.queue,
                opts.workflowName,
                opts.executionId,
                opts.deferredName,
                opts.exit,
              );
            }),

          // The wake timer lives only for the lifetime of this engine layer:
          // a workflow sleeping on a durable clock past a layer restart stays
          // suspended until resumed explicitly. Persisting scheduled wakeups
          // is future work; this method does not pretend otherwise.
          scheduleClock: (workflow, opts) => {
            const queue = queueFor(workflow);
            return FiberMap.run(
              clocks,
              `${opts.executionId}/${opts.clock.name}`,
              Effect.delay(
                completeDeferredInternal(
                  queue,
                  workflow._tag,
                  opts.executionId,
                  opts.clock.deferred.name,
                  Exit.void,
                ),
                opts.clock.duration,
              ),
              { onlyIfMissing: true },
            ).pipe(Effect.asVoid);
          },
        });

        for (const config of queues) {
          for (let slot = 0; slot < config.concurrency; slot += 1) {
            yield* Effect.forkScoped(workerLoop(engine, config));
          }
        }

        return engine;
      }),
    ),
} satisfies {
  readonly inQueue: (queueName: string) => <W extends Workflow.Any>(workflow: W) => W;
  readonly layer: (
    options: LayerOptions,
  ) => Layer.Layer<WorkflowEngine.WorkflowEngine, never, SqlClient.SqlClient>;
};
