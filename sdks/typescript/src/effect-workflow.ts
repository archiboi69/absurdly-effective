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
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import type * as SqlError from "effect/unstable/sql/SqlError";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { Activity, DurableDeferred, Workflow, WorkflowEngine } from "effect/unstable/workflow";
import * as os from "os";

import {
  absurdWorkflowStore,
  type ClaimedRow,
  type TaskSnapshotRow,
} from "./effect-workflow-store.ts";

// The upstream `WorkflowEngine.Encoded` contract mandates `object`/`unknown`
// at its persistence seam (payloads, checkpoints, exits), and Effect's own
// reference implementations of that contract are lint-excluded vendored code.
// These signatures are the contract speaking, not design failures; everything
// outside them passes all rules.
// oxlint-disable anti-slop/no-object-parameters
// oxlint-disable anti-slop/no-unknown-parameters
// oxlint-disable anti-slop/no-chained-type-assertions
// oxlint-disable anti-slop/require-safety-comment-for-type-assertion
// oxlint-disable anti-slop/no-runtime-typeof
// oxlint-disable effecttsgo/any-unknown-in-error-context

/**
 * Annotation key holding the Absurd queue a workflow is bound to via
 * `AbsurdWorkflowEngine.inQueue`.
 */
class QueueAnnotation extends Context.Reference<string>(
  "absurd-sdk/AbsurdWorkflowEngine/QueueAnnotation",
  { defaultValue: () => "" },
) {}

interface ActiveClaim {
  readonly queue: string;
  readonly taskId: string;
  /**
   * The exact run ID this worker claimed.
   *
   * Persistence operations fence themselves to this run: checkpoints are
   * written under its ID, so a stale worker whose lease was lost cannot
   * commit state underneath its replacement (Absurd rejects writes owned by
   * superseded runs).
   */
  readonly runId: string;
}

/**
 * The claim a worker fiber is currently executing.
 *
 * Provided around handler replay; persistence helpers read it to target the
 * claimed queue/task/run triple without rereading mutable ownership columns.
 */
class ClaimContext extends Context.Service<ClaimContext, ActiveClaim>()(
  "absurd-sdk/effect-workflow/ClaimContext",
) {}

const MAX_QUEUE_NAME_LENGTH = 57;
const DEFAULT_CLAIM_TIMEOUT = Duration.seconds(120);
const MIN_CLAIM_TIMEOUT_SECONDS = 1;
const UNKNOWN_TASK_DEFER_BASE_SECONDS = 15;
const UNKNOWN_TASK_DEFER_JITTER_SECONDS = 15;
const SUSPEND_RETRY_SECONDS = 0.25;

/**
 * Normalizes a claim lease to whole seconds (Absurd's `make_interval` takes
 * integer seconds), clamped to a positive minimum.
 */
const normalizeClaimTimeoutSeconds = (input: Duration.Input | undefined): number => {
  const millis = Duration.toMillis(Duration.fromInputUnsafe(input ?? DEFAULT_CLAIM_TIMEOUT));
  return Math.max(MIN_CLAIM_TIMEOUT_SECONDS, Math.round(millis / 1000));
};

/**
 * Heartbeat cadence derived from the lease: renew at half the lease so a
 * killed worker holds a stale claim for at most one extra interval.
 */
const heartbeatIntervalMillis = (claimTimeoutSeconds: number): number =>
  Math.max(500, Math.floor((claimTimeoutSeconds * 1000) / 2));

interface QueueConfig {
  readonly name: string;
  readonly concurrency: number;
  readonly pollIntervalMillis: number;
  readonly claimTimeoutSeconds: number;
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
  /**
   * Claim lease granted to this worker when it picks up a run, in Effect
   * `Duration`. The lease is renewed by a heartbeat at half this interval
   * while a handler is in flight; losing the heartbeat interrupts the
   * handler, and if the worker dies another worker may reclaim the run once
   * the lease expires. Absurd leases whole seconds, so values are rounded
   * and clamped to a minimum of one second.
   *
   * @default 120s
   */
  readonly claimTimeout?: Duration.Input | undefined;
}

export interface LayerOptions {
  readonly queues: Readonly<Record<string, QueueOptions>>;
}

interface Registration {
  readonly workflow: Workflow.Any;
  readonly queue: string;
  /** Persistence codecs derived once from the workflow's own schemas. */
  readonly codecs: WorkflowPersistenceCodecs;
  /** Registration-time context; provides schema encoding/decoding services. */
  readonly services: Context.Context<never>;
  readonly execute: (
    payload: object,
    executionId: string,
  ) => Effect.Effect<
    unknown,
    unknown,
    WorkflowEngine.WorkflowInstance | WorkflowEngine.WorkflowEngine
  >;
}

// ---------------------------------------------------------------------------
// Persistence codecs
//
// `WorkflowEngine.makeUnsafe` fixes the value domain at every seam
// (`unstable/workflow/WorkflowEngine.ts`): payloads and registered-handler
// results arrive on the Type side, while activity exits (`executeEncoded`
// pre-encodes) and deferred completions (`deferred.exitSchema` pre-encodes)
// arrive already encoded — `makeUnsafe` applies the Type-side decode itself.
// The adapter therefore keeps three explicit persistence paths:
//
// 1. Workflow payload/result: Type side ↔ canonical JSON through codecs
//    derived from the workflow's own schemas (`toCodecJson`), so schema
//    transformations survive storage. Codec failures defect — invalid
//    persisted data must never silently degrade into raw values.
// 2. Activity checkpoints: preserve the encoded success/error values exactly,
//    validating only the surrounding Exit structure.
// 3. Deferred completions: same structural contract as activities.
//
// Paths 2 and 3 share one envelope codec with service-free slots
// (`Unknown`/`Defect`), which serializes defects and reconstructs `Error`
// instances without touching encoded payloads.
// ---------------------------------------------------------------------------

/** Canonical JSON codec pair for one workflow's Type-side persistence. */
export interface WorkflowPersistenceCodecs {
  readonly payload: ReturnType<typeof Schema.toCodecJson>;
  readonly result: ReturnType<typeof Schema.toCodecJson>;
}

export const workflowPersistenceCodecs = (workflow: Workflow.Any): WorkflowPersistenceCodecs => ({
  payload: Schema.toCodecJson(workflow.payloadSchema),
  result: Schema.toCodecJson(
    Workflow.Result({
      success: workflow.successSchema,
      error: workflow.errorSchema,
    }),
  ),
});

/**
 * Structural Exit envelope for the already-encoded boundaries (activities and
 * deferreds). `Unknown` slots pass encoded values through untouched; `Defect`
// serializes defects on write and reconstructs Error instances on read.
 */
export const structuralExitCodec = Schema.toCodecJson(
  Schema.Exit(Schema.Unknown, Schema.Unknown, Schema.Defect()),
);

/** Normalizes nullish top-level success values, mirroring makeUnsafe's toJsonExit. */
export const exitWithNullishValues = (
  exit: Exit.Exit<unknown, unknown>,
): Exit.Exit<unknown, unknown> => Exit.map(exit, (value) => value ?? null);

export type StructuralExitEncoded = Schema.Codec.Encoded<typeof structuralExitCodec>;

export const encodeStructuralExit = (exit: Exit.Exit<unknown, unknown>): StructuralExitEncoded =>
  exit.pipe(exitWithNullishValues, Schema.encodeSync(structuralExitCodec));

export const decodeStructuralExit = (stored: unknown): Exit.Exit<unknown, unknown> =>
  Schema.decodeUnknownSync(structuralExitCodec)(stored);

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
const clockDeadlineCheckpointName = (deferredName: string): string => `$clock:${deferredName}`;
const INTERRUPT_CHECKPOINT_NAME = "$workflow:interrupt";
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

/** Provides an Effect-native durable `WorkflowEngine` backed by Absurd. */
export const AbsurdWorkflowEngine = {
  inQueue:
    (queueName: string) =>
    <W extends Workflow.Any>(workflow: W): W => {
      validateQueueName(queueName);
      // SAFETY: Effect's erased `Workflow.Any` omits `annotate`, while every
      // concrete workflow value has it and annotation preserves the exact W.
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
        const store = absurdWorkflowStore(sql);
        const fatal = <A>(effect: Effect.Effect<A, SqlError.SqlError>): Effect.Effect<A> =>
          Effect.orDie(effect);

        const queueConfigs = Object.entries(options.queues).map(([name, queueOptions]) => ({
          name: validateQueueName(name),
          concurrency: Math.max(1, Math.floor(queueOptions.concurrency ?? 1)),
          pollIntervalMillis: Duration.toMillis(
            Duration.fromInputUnsafe(queueOptions.pollInterval ?? Duration.millis(250)),
          ),
          claimTimeoutSeconds: normalizeClaimTimeoutSeconds(queueOptions.claimTimeout),
        }));
        const queueConfigByName = new Map(queueConfigs.map((config) => [config.name, config]));
        const queueReady = new Map<string, Deferred.Deferred<void>>();
        for (const config of queueConfigs) {
          queueReady.set(config.name, yield* Deferred.make<void>());
        }

        const workerId = `absurd-effect:${os.hostname?.() ?? "host"}:${process.pid}`;
        const registrations = new Map<string, Registration>();
        const codecCache = new Map<string, WorkflowPersistenceCodecs>();

        const queueFor = (workflow: Workflow.Any): string => {
          const annotation = Context.getOption(workflow.annotations, QueueAnnotation);
          if (Option.isNone(annotation) || annotation.value === "") {
            throw new Error(
              `Workflow "${workflow._tag}" has no Absurd queue annotation; wrap it with AbsurdWorkflowEngine.inQueue.`,
            );
          }
          if (!queueConfigByName.has(annotation.value)) {
            throw new Error(
              `Workflow "${workflow._tag}" is bound to queue "${annotation.value}", but that queue is absent from AbsurdWorkflowEngine.layer.`,
            );
          }
          return annotation.value;
        };

        const codecsFor = (workflow: Workflow.Any): WorkflowPersistenceCodecs => {
          const cached = codecCache.get(workflow._tag);
          if (cached !== undefined) return cached;
          const codecs = workflowPersistenceCodecs(workflow);
          codecCache.set(workflow._tag, codecs);
          return codecs;
        };

        /**
         * `Workflow.Any` necessarily erases schema service requirements. The
         * context captured at registration (or at the public call site) is the
         * exact context from which those schemas came, so restoring it here is
         * the single type-erasure boundary for schema operations.
         */
        const runCodecWith = <A>(
          effect: Effect.Effect<A, unknown, unknown>,
          services: Context.Context<never>,
        ): Effect.Effect<A> =>
          Effect.provideContext(effect, services as Context.Context<unknown>).pipe(Effect.orDie);

        const runCodec = <A>(effect: Effect.Effect<A, unknown, unknown>): Effect.Effect<A> =>
          Effect.flatMap(Effect.context<never>(), (services) => runCodecWith(effect, services));

        const requireClaim = Effect.fnUntraced(function* (): Effect.fn.Return<ActiveClaim> {
          const claim = yield* Effect.serviceOption(ClaimContext);
          if (Option.isNone(claim)) {
            return yield* Effect.die(
              "This operation is only valid while executing a claimed Absurd workflow run.",
            );
          }
          return claim.value;
        });

        const registerWorkflow = Effect.fnUntraced(function* (
          workflow: Workflow.Any,
          execute: Registration["execute"],
        ): Effect.fn.Return<void, never, Scope.Scope> {
          const queue = queueFor(workflow);
          const existing = registrations.get(workflow._tag);
          if (existing !== undefined) {
            return yield* Effect.die(
              `Workflow "${workflow._tag}" is already registered in queue "${existing.queue}".`,
            );
          }

          const registration: Registration = {
            workflow,
            queue,
            codecs: codecsFor(workflow),
            services: yield* Effect.context<never>(),
            execute,
          };
          registrations.set(workflow._tag, registration);

          yield* Scope.addFinalizer(
            yield* Effect.scope,
            Effect.sync(() => {
              if (registrations.get(workflow._tag) === registration) {
                registrations.delete(workflow._tag);
              }
            }),
          );
          yield* Deferred.succeed(queueReady.get(queue)!, undefined);
        });

        const deferUnknownRun = Effect.fnUntraced(function* (
          queue: string,
          runId: string,
        ): Effect.fn.Return<void> {
          yield* fatal(
            store.scheduleRunInSeconds(
              queue,
              runId,
              UNKNOWN_TASK_DEFER_BASE_SECONDS +
                deterministicJitterSeconds(runId, UNKNOWN_TASK_DEFER_JITTER_SECONDS),
            ),
          );
        });

        const heartbeat = (claim: ActiveClaim, claimTimeoutSeconds: number) =>
          Effect.sleep(Duration.millis(heartbeatIntervalMillis(claimTimeoutSeconds))).pipe(
            Effect.andThen(
              Effect.suspend(() =>
                fatal(store.extendClaim(claim.queue, claim.runId, claimTimeoutSeconds)),
              ),
            ),
            Effect.forever,
          );

        const parkSuspended = Effect.fnUntraced(function* (
          registration: Registration,
          claim: ActiveClaim,
          executionId: string,
          instance: WorkflowEngine.WorkflowInstance["Service"],
        ): Effect.fn.Return<void> {
          const interruptRequested = yield* fatal(
            store.checkpointState(claim.queue, claim.taskId, INTERRUPT_CHECKPOINT_NAME),
          );
          if (Option.isSome(interruptRequested)) {
            yield* fatal(store.scheduleRunInSeconds(claim.queue, claim.runId, 0));
            return;
          }

          for (const deferredName of instance.awaitedDeferreds) {
            const completed = yield* fatal(
              store.checkpointState(
                claim.queue,
                claim.taskId,
                deferredCheckpointName(deferredName),
              ),
            );
            if (Option.isSome(completed)) continue;

            const deadline = yield* fatal(
              store.checkpointState(
                claim.queue,
                claim.taskId,
                clockDeadlineCheckpointName(deferredName),
              ),
            );
            if (Option.isSome(deadline) && typeof deadline.value === "number") {
              const now = yield* Clock.currentTimeMillis;
              yield* fatal(
                store.scheduleRunInSeconds(
                  claim.queue,
                  claim.runId,
                  Math.max(0, (deadline.value - now) / 1000),
                ),
              );
              const racedInterrupt = yield* fatal(
                store.checkpointState(claim.queue, claim.taskId, INTERRUPT_CHECKPOINT_NAME),
              );
              if (Option.isSome(racedInterrupt)) {
                yield* fatal(
                  store.requestTaskInterrupt(claim.queue, claim.taskId, INTERRUPT_CHECKPOINT_NAME),
                );
              }
              return;
            }

            const wait = yield* fatal(
              store.awaitEvent(
                claim.queue,
                claim.taskId,
                claim.runId,
                deferredCheckpointName(deferredName),
                deferredEventName(registration.workflow._tag, executionId, deferredName),
              ),
            );
            if (wait.should_suspend) {
              const racedInterrupt = yield* fatal(
                store.checkpointState(claim.queue, claim.taskId, INTERRUPT_CHECKPOINT_NAME),
              );
              if (Option.isSome(racedInterrupt)) {
                yield* fatal(
                  store.requestTaskInterrupt(claim.queue, claim.taskId, INTERRUPT_CHECKPOINT_NAME),
                );
              }
              return;
            }

            // The event won the read/register race and committed a checkpoint;
            // replay immediately so the handler can observe it.
            yield* fatal(store.scheduleRunInSeconds(claim.queue, claim.runId, 0));
            return;
          }

          // `SuspendOnFailure` and explicit suspension have no event to attach
          // yet. Keep the run replayable without a hot polling loop.
          yield* fatal(store.scheduleRunInSeconds(claim.queue, claim.runId, SUSPEND_RETRY_SECONDS));
          const racedInterrupt = yield* fatal(
            store.checkpointState(claim.queue, claim.taskId, INTERRUPT_CHECKPOINT_NAME),
          );
          if (Option.isSome(racedInterrupt)) {
            yield* fatal(
              store.requestTaskInterrupt(claim.queue, claim.taskId, INTERRUPT_CHECKPOINT_NAME),
            );
          }
        });

        let engine!: WorkflowEngine.WorkflowEngine["Service"];

        const processClaim = Effect.fnUntraced(function* (
          config: QueueConfig,
          claimed: ClaimedRow,
        ): Effect.fn.Return<void> {
          const registration = registrations.get(claimed.task_name);
          if (registration === undefined) {
            return yield* deferUnknownRun(config.name, claimed.run_id);
          }

          const executionId = yield* fatal(store.executionIdForTask(config.name, claimed.task_id));
          if (Option.isNone(executionId)) {
            return yield* deferUnknownRun(config.name, claimed.run_id);
          }

          const claim: ActiveClaim = {
            queue: config.name,
            taskId: claimed.task_id,
            runId: claimed.run_id,
          };
          const payload = (yield* runCodecWith(
            Schema.decodeUnknownEffect(registration.codecs.payload)(claimed.params),
            registration.services,
          )) as object;
          const instance = WorkflowEngine.WorkflowInstance.initial(
            registration.workflow,
            executionId.value,
          );

          const interruptRequested = yield* fatal(
            store.checkpointState(config.name, claimed.task_id, INTERRUPT_CHECKPOINT_NAME),
          );

          const handler = registration.execute(payload, executionId.value).pipe(
            Effect.onInterrupt(() =>
              Effect.sync(() => {
                // Process shutdown or lease loss is replay, not workflow
                // failure: do not run compensations for infrastructure churn.
                instance.suspended = true;
              }),
            ),
            Effect.onExit(() => {
              if (Option.isNone(interruptRequested)) return Effect.void;
              instance.interrupted = true;
              instance.suspended = false;
              return Effect.withFiber((fiber) => fiber.pipe(Fiber.interrupt, Effect.interruptible));
            }),
            Workflow.intoResult,
            Effect.provideService(WorkflowEngine.WorkflowEngine, engine),
            Effect.provideService(WorkflowEngine.WorkflowInstance, instance),
            Effect.provideService(ClaimContext, ClaimContext.of(claim)),
          );
          const lease = heartbeat(claim, config.claimTimeoutSeconds).pipe(
            Effect.tapCause((cause) =>
              Cause.hasInterruptsOnly(cause)
                ? Effect.void
                : Effect.sync(() => {
                    instance.suspended = true;
                  }),
            ),
          );

          const outcome = yield* Effect.raceFirst(handler, lease);
          if (outcome._tag === "Complete") {
            const stored = yield* runCodecWith(
              Schema.encodeEffect(registration.codecs.result)(outcome),
              registration.services,
            );
            yield* fatal(store.completeRun(config.name, claimed.run_id, stored));
            return;
          }

          yield* parkSuspended(registration, claim, executionId.value, instance);
          // A suspended run is reconstructed by replay. Close this process-local
          // scope successfully to release resources without firing compensation.
          yield* Scope.close(instance.scope, Exit.void);
        });

        const workerLoop = (config: QueueConfig) =>
          Effect.forever(
            Effect.suspend(() =>
              fatal(store.claimTask(config.name, workerId, config.claimTimeoutSeconds)),
            ).pipe(
              Effect.flatMap(
                Option.match({
                  onNone: () => Effect.sleep(Duration.millis(config.pollIntervalMillis)),
                  onSome: (claimed) =>
                    processClaim(config, claimed).pipe(
                      Effect.catchCause((cause) =>
                        Effect.logError(
                          `[absurd-effect] workflow run ${claimed.task_name} (${claimed.task_id}/${claimed.run_id}) failed`,
                          cause,
                        ),
                      ),
                    ),
                }),
              ),
            ),
          );

        const decodeCompleted = Effect.fnUntraced(function* (
          workflow: Workflow.Any,
          snapshot: TaskSnapshotRow,
        ): Effect.fn.Return<Workflow.Result<unknown, unknown>> {
          return (yield* runCodec(
            Schema.decodeUnknownEffect(codecsFor(workflow).result)(snapshot.result),
          )) as Workflow.Result<unknown, unknown>;
        });

        const resultFromSnapshot = (
          workflow: Workflow.Any,
          snapshot: TaskSnapshotRow,
        ): Effect.Effect<Workflow.Result<unknown, unknown>> => {
          if (snapshot.state === "completed") return decodeCompleted(workflow, snapshot);
          if (snapshot.state === "failed" || snapshot.state === "cancelled") {
            return Effect.die(
              new Error(`Workflow "${workflow._tag}" ended in state "${snapshot.state}".`, {
                cause: snapshot.failure_reason,
              }),
            );
          }
          return Effect.succeed(Workflow.Suspended.make({}));
        };

        const cancelByExecutionId = Effect.fnUntraced(function* (
          workflow: Workflow.Any,
          executionId: string,
        ): Effect.fn.Return<void> {
          const task = yield* fatal(store.taskIdForExecution(queueFor(workflow), executionId));
          if (Option.isSome(task)) {
            yield* fatal(store.cancelTask(queueFor(workflow), task.value.task_id));
          }
        });

        const interruptByExecutionId = Effect.fnUntraced(function* (
          workflow: Workflow.Any,
          executionId: string,
        ): Effect.fn.Return<void> {
          const queue = queueFor(workflow);
          const task = yield* fatal(store.taskIdForExecution(queue, executionId));
          if (Option.isSome(task)) {
            yield* fatal(
              store.requestTaskInterrupt(queue, task.value.task_id, INTERRUPT_CHECKPOINT_NAME),
            );
          }
        });

        const queueForDeferred = Effect.fnUntraced(function* (
          workflowName: string,
          executionId: string,
        ): Effect.fn.Return<string> {
          const registered = registrations.get(workflowName);
          if (registered !== undefined) return registered.queue;

          const matches: Array<string> = [];
          for (const config of queueConfigs) {
            const task = yield* fatal(store.taskIdForExecution(config.name, executionId));
            if (Option.isSome(task)) matches.push(config.name);
          }
          if (matches.length !== 1) {
            return yield* Effect.die(
              `Expected exactly one Absurd queue for workflow "${workflowName}" execution "${executionId}", found ${matches.length}.`,
            );
          }
          return matches[0]!;
        });

        engine = WorkflowEngine.makeUnsafe({
          register: registerWorkflow,

          execute: (workflow, opts) =>
            Effect.gen(function* () {
              const queue = queueFor(workflow);
              const storedPayload = yield* runCodec(
                Schema.encodeEffect(codecsFor(workflow).payload)(opts.payload),
              );
              // SAFETY: workflow payload schemas are structs, so their encoded
              // persistence representation is an object.
              const spawned = yield* fatal(
                store.spawnTask(queue, workflow._tag, storedPayload as object, {
                  idempotency_key: opts.executionId,
                  retry_strategy: { kind: "fixed", base_seconds: 1 },
                }),
              );
              if (opts.discard) return;

              const snapshot = yield* fatal(store.taskResult(queue, spawned.task_id));
              if (Option.isNone(snapshot)) {
                return yield* Effect.die(
                  `Absurd task for execution "${opts.executionId}" vanished from queue "${queue}".`,
                );
              }
              return yield* resultFromSnapshot(workflow, snapshot.value);
            }) as Effect.Effect<
              typeof opts.discard extends true ? void : Workflow.Result<unknown, unknown>
            >,

          poll: Effect.fnUntraced(function* (
            workflow: Workflow.Any,
            executionId: string,
          ): Effect.fn.Return<Option.Option<Workflow.Result<unknown, unknown>>> {
            const queue = queueFor(workflow);
            const task = yield* fatal(store.taskIdForExecution(queue, executionId));
            if (Option.isNone(task)) return Option.none();
            const snapshot = yield* fatal(store.taskResult(queue, task.value.task_id));
            if (Option.isNone(snapshot)) return Option.none();
            if (snapshot.value.state === "pending" || snapshot.value.state === "running") {
              return Option.none();
            }
            return Option.some(yield* resultFromSnapshot(workflow, snapshot.value));
          }),

          interrupt: interruptByExecutionId,
          interruptUnsafe: cancelByExecutionId,

          resume: Effect.fnUntraced(function* (workflow, executionId) {
            const task = yield* fatal(store.taskIdForExecution(queueFor(workflow), executionId));
            if (Option.isSome(task) && task.value.last_attempt_run !== null) {
              yield* fatal(store.resumeRun(queueFor(workflow), task.value.last_attempt_run));
            }
          }),

          activityExecute: Effect.fnUntraced(function* (activity: Activity.Any, attempt: number) {
            const parent = yield* WorkflowEngine.WorkflowInstance;
            const claim = yield* requireClaim();
            const checkpoint = activityCheckpointName(activity.name, attempt);
            const stored = yield* fatal(
              store.checkpointState(claim.queue, claim.taskId, checkpoint),
            );
            if (Option.isSome(stored)) {
              return new Workflow.Complete({ exit: decodeStructuralExit(stored.value) });
            }

            const activityInstance = WorkflowEngine.WorkflowInstance.initial(
              parent.workflow,
              parent.executionId,
            );
            const result = yield* activity.executeEncoded.pipe(
              Workflow.intoResult,
              Effect.provideService(WorkflowEngine.WorkflowInstance, activityInstance),
              Effect.provideService(Activity.CurrentAttempt, attempt),
            );
            if (result._tag === "Suspended") {
              yield* Scope.close(activityInstance.scope, Exit.void);
              return result;
            }

            yield* fatal(
              store.setCheckpointState(
                claim.queue,
                claim.taskId,
                checkpoint,
                encodeStructuralExit(result.exit),
                claim.runId,
              ),
            );
            return new Workflow.Complete({ exit: exitWithNullishValues(result.exit) });
          }),

          deferredResult: Effect.fnUntraced(function* (deferred: DurableDeferred.Any) {
            const claim = yield* requireClaim();
            const stored = yield* fatal(
              store.checkpointState(
                claim.queue,
                claim.taskId,
                deferredCheckpointName(deferred.name),
              ),
            );
            return Option.map(stored, decodeStructuralExit);
          }),

          deferredDone: Effect.fnUntraced(function* (opts) {
            const queue = yield* queueForDeferred(opts.workflowName, opts.executionId);
            // `emit_event` is first-write-wins and atomically wakes an existing
            // waiter (including committing its checkpoint). If no waiter exists
            // yet, the cached event closes the registration race on replay.
            yield* fatal(
              store.emitEvent(
                queue,
                deferredEventName(opts.workflowName, opts.executionId, opts.deferredName),
                encodeStructuralExit(opts.exit as Exit.Exit<unknown, unknown>),
              ),
            );
          }),

          scheduleClock: Effect.fnUntraced(function* (workflow, opts) {
            const claim = yield* requireClaim();
            const deferredName = opts.clock.deferred.name;
            const deadlineCheckpoint = clockDeadlineCheckpointName(deferredName);
            const existing = yield* fatal(
              store.checkpointState(claim.queue, claim.taskId, deadlineCheckpoint),
            );
            const now = yield* Clock.currentTimeMillis;
            const deadline =
              Option.isSome(existing) && typeof existing.value === "number"
                ? existing.value
                : now + Duration.toMillis(opts.clock.duration);

            if (Option.isNone(existing)) {
              yield* fatal(
                store.setCheckpointState(
                  claim.queue,
                  claim.taskId,
                  deadlineCheckpoint,
                  deadline,
                  claim.runId,
                ),
              );
            }
            if (now < deadline) return;

            const completion = encodeStructuralExit(Exit.succeed(null));
            yield* fatal(
              store.setCheckpointState(
                claim.queue,
                claim.taskId,
                deferredCheckpointName(deferredName),
                completion,
                claim.runId,
              ),
            );
            yield* fatal(
              store.emitEvent(
                claim.queue,
                deferredEventName(workflow._tag, opts.executionId, deferredName),
                completion,
              ),
            );
          }),
        });

        for (const config of queueConfigs) {
          yield* queueReady
            .get(config.name)!
            .pipe(
              Deferred.await,
              Effect.andThen(
                Effect.forEach(
                  Array.from({ length: config.concurrency }),
                  () => Effect.forkScoped(workerLoop(config)),
                  { concurrency: "unbounded", discard: true },
                ),
              ),
              Effect.forkScoped,
            );
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
