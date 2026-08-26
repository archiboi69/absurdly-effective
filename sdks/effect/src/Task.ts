import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { TaskStore, TaskStoreError, type StoredTaskStatus } from "./TaskStore.ts";

// Task payloads and results cross an untyped JSON boundary. Their schemas are
// the single validation and transformation boundary.
// oxlint-disable anti-slop/no-unknown-parameters
// Task.make deliberately mirrors Effect's schema-first definition modules.
// oxlint-disable effecttsgo/missing-pipeable-signature

type TaskIdBrand<Name extends string> = `absurd-effect/TaskId/${Name}`;

export type IdSchema<Name extends string> = Schema.brand<typeof Schema.String, TaskIdBrand<Name>>;

export type TaskId<Name extends string = string> = IdSchema<Name>["Type"];

export type Retry =
  | { readonly _tag: "None" }
  | { readonly _tag: "Fixed"; readonly delay: Duration.Input }
  | {
      readonly _tag: "Exponential";
      readonly base: Duration.Input;
      readonly factor?: number | undefined;
      readonly maxDelay?: Duration.Input | undefined;
    };

export interface Cancellation {
  readonly maxDuration?: Duration.Input | undefined;
  readonly maxDelay?: Duration.Input | undefined;
}

export interface EnqueueOptions {
  readonly maxAttempts?: number | undefined;
  readonly retry?: Retry | undefined;
  readonly headers?: Schema.JsonObject | undefined;
  readonly cancellation?: Cancellation | undefined;
  readonly idempotencyKey?: string | undefined;
}

export interface RoutedEnqueueOptions extends EnqueueOptions {
  readonly queue: string;
}

export class TaskPayloadEncodeError extends Schema.TaggedError<TaskPayloadEncodeError>()(
  "TaskPayloadEncodeError",
  {
    taskName: Schema.String,
    cause: Schema.Defect(),
  },
) {}

export class TaskPayloadDecodeError extends Schema.TaggedError<TaskPayloadDecodeError>()(
  "TaskPayloadDecodeError",
  {
    taskName: Schema.String,
    cause: Schema.Defect(),
  },
) {}

export class TaskSuccessEncodeError extends Schema.TaggedError<TaskSuccessEncodeError>()(
  "TaskSuccessEncodeError",
  {
    taskName: Schema.String,
    cause: Schema.Defect(),
  },
) {}

export class TaskSuccessDecodeError extends Schema.TaggedError<TaskSuccessDecodeError>()(
  "TaskSuccessDecodeError",
  {
    taskName: Schema.String,
    taskId: Schema.String,
    cause: Schema.Defect(),
  },
) {}

export type Status<Success> =
  | { readonly _tag: "NotFound" }
  | { readonly _tag: "Pending" }
  | { readonly _tag: "Running" }
  | { readonly _tag: "Sleeping" }
  | { readonly _tag: "Completed"; readonly value: Success }
  | { readonly _tag: "Failed"; readonly failure: Schema.Json }
  | { readonly _tag: "Cancelled" };

export type PayloadInput = Schema.Top | Schema.Struct.Fields;

export type PayloadSchema<Payload extends PayloadInput> = Payload extends Schema.Struct.Fields
  ? Schema.Struct<Payload>
  : Payload;

export interface Config<Payload extends PayloadInput, Success extends Schema.Top> {
  readonly queue: string;
  readonly payload: Payload;
  readonly success?: Success | undefined;
  readonly idempotencyKey?: ((payload: PayloadSchema<Payload>["Type"]) => string) | undefined;
  readonly maxAttempts?: number | undefined;
  readonly cancellation?: Cancellation | undefined;
}

export interface Handler<
  Name extends string,
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error,
  Requirements,
> {
  readonly task: {
    readonly name: Name;
    readonly queue: string;
    readonly maxAttempts: number | undefined;
    readonly cancellation: Cancellation | undefined;
  };
  readonly execute: (
    payload: unknown,
  ) => Effect.Effect<
    unknown,
    Error | TaskPayloadDecodeError | TaskSuccessEncodeError,
    Requirements | Payload["DecodingServices"] | Success["EncodingServices"]
  >;
}

export interface AnyHandler {
  readonly task: {
    readonly name: string;
    readonly queue: string;
    readonly maxAttempts: number | undefined;
    readonly cancellation: Cancellation | undefined;
  };
  readonly execute: (payload: unknown) => Effect.Effect<unknown, unknown, unknown>;
}

export type HandlerRequirements<Handler extends AnyHandler> = Handler["execute"] extends (
  payload: unknown,
) => Effect.Effect<unknown, unknown, infer Requirements>
  ? Requirements
  : never;

export interface Definition<
  Name extends string,
  Payload extends Schema.Top,
  Success extends Schema.Top,
> {
  readonly name: Name;
  readonly queue: string;
  readonly payloadSchema: Payload;
  readonly successSchema: Success;
  readonly idSchema: IdSchema<Name>;
  readonly enqueue: (
    payload: Payload["~type.make.in"],
    options?: EnqueueOptions,
  ) => Effect.Effect<
    TaskId<Name>,
    TaskPayloadEncodeError | TaskStoreError,
    TaskStore | Payload["EncodingServices"]
  >;
  readonly status: (
    taskId: TaskId<Name>,
  ) => Effect.Effect<
    Status<Success["Type"]>,
    TaskStoreError | TaskSuccessDecodeError,
    TaskStore | Success["DecodingServices"]
  >;
  readonly handler: <Error, Requirements>(
    execute: (payload: Payload["Type"]) => Effect.Effect<Success["Type"], Error, Requirements>,
  ) => Handler<Name, Payload, Success, Error, Requirements>;
}

const decodeStatus = <Success extends Schema.Top>(
  name: string,
  taskId: string,
  success: Success,
  stored: StoredTaskStatus,
): Effect.Effect<Status<Success["Type"]>, TaskSuccessDecodeError, Success["DecodingServices"]> => {
  if (stored._tag !== "Completed") return Effect.succeed(stored);
  return Schema.decodeUnknownEffect(Schema.toCodecJson(success))(stored.value).pipe(
    Effect.map((value) => ({ _tag: "Completed" as const, value })),
    Effect.mapError((cause) => TaskSuccessDecodeError.make({ taskName: name, taskId, cause })),
  );
};

const constructPayload = <Payload extends Schema.Top>(
  schema: Payload,
  input: Payload["~type.make.in"],
) => schema.makeEffect(input);

export const make = <
  const Name extends string,
  Payload extends PayloadInput,
  Success extends Schema.Top = typeof Schema.Void,
>(
  name: Name,
  config: Config<Payload, Success>,
): Definition<Name, PayloadSchema<Payload>, Success> => {
  // SAFETY: the conditional matches PayloadSchema exactly: existing schemas
  // pass through, while field records are normalized with Schema.Struct.
  const payloadSchema = (
    Schema.isSchema(config.payload) ? config.payload : Schema.Struct(config.payload)
  ) as PayloadSchema<Payload>;
  // SAFETY: Success defaults to Schema.Void exactly when config.success is
  // absent; when it is present, nullish coalescing returns that same Success.
  const successSchema = (config.success ?? Schema.Void) as Success;
  const payloadCodec = Schema.toCodecJson(payloadSchema);
  const successCodec = Schema.toCodecJson(successSchema);
  const idSchema = Schema.String.pipe(Schema.brand(`absurd-effect/TaskId/${name}`));

  const enqueue: Definition<Name, PayloadSchema<Payload>, Success>["enqueue"] = Effect.fn(
    `${name}.enqueue`,
  )(function* (input, options = {}) {
    const store = yield* TaskStore;
    const payload = yield* constructPayload(payloadSchema, input).pipe(
      Effect.mapError((cause) => TaskPayloadEncodeError.make({ taskName: name, cause })),
    );
    const encoded = yield* Schema.encodeEffect(payloadCodec)(payload).pipe(
      Effect.mapError((cause) => TaskPayloadEncodeError.make({ taskName: name, cause })),
    );
    const idempotencyKey = options.idempotencyKey ?? config.idempotencyKey?.(payload);
    const routedOptions: RoutedEnqueueOptions = {
      ...options,
      queue: config.queue,
      idempotencyKey,
    };
    const taskId = yield* store.enqueue({ name, payload: encoded, options: routedOptions });
    return idSchema.make(taskId);
  });

  const status: Definition<Name, PayloadSchema<Payload>, Success>["status"] = Effect.fn(
    `${name}.status`,
  )(function* (taskId) {
    const store = yield* TaskStore;
    return yield* decodeStatus(
      name,
      taskId,
      successSchema,
      yield* store.status({ id: taskId, name, queue: config.queue }),
    );
  });

  return {
    name,
    queue: config.queue,
    payloadSchema,
    successSchema,
    idSchema,
    enqueue,
    status,
    handler: (execute) => ({
      task: {
        name,
        queue: config.queue,
        maxAttempts: config.maxAttempts,
        cancellation: config.cancellation,
      },
      execute: Effect.fn(`${name}.handler`)(function* (payload: unknown) {
        const decoded = yield* Schema.decodeUnknownEffect(payloadCodec)(payload).pipe(
          Effect.mapError((cause) => TaskPayloadDecodeError.make({ taskName: name, cause })),
        );
        const value = yield* execute(decoded);
        return yield* Schema.encodeEffect(successCodec)(value).pipe(
          Effect.mapError((cause) => TaskSuccessEncodeError.make({ taskName: name, cause })),
        );
      }),
    }),
  };
};
