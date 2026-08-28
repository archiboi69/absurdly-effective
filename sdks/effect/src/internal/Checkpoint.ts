import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import type { ExecuteStep } from "../CurrentTask.ts";
import {
  StepDecodeError,
  StepEncodeError,
  type Options,
  type StepPersistenceError,
} from "../Step.ts";

// Stored step values cross an untyped JSON boundary owned by the success Schema.
// oxlint-disable anti-slop/no-unknown-parameters

export interface StoredStep {
  readonly value: Option.Option<unknown>;
  readonly complete: (value: unknown) => Effect.Effect<void, StepPersistenceError>;
}

export type BeginStep = (name: string) => Effect.Effect<StoredStep, StepPersistenceError>;

export const makeStep = (begin: BeginStep): ExecuteStep =>
  Effect.fn("Step.make")(function* <Success extends Schema.Top, Error, Requirements>(
    options: Options<Success, Error, Requirements>,
  ) {
    const stored = yield* begin(options.name);
    const codec = Schema.toCodecJson(options.success);
    if (Option.isSome(stored.value)) {
      return yield* Schema.decodeUnknownEffect(codec)(stored.value.value).pipe(
        Effect.mapError((cause) => StepDecodeError.make({ stepName: options.name, cause })),
      );
    }

    const value = yield* options.execute;
    const encoded = yield* Schema.encodeEffect(codec)(value).pipe(
      Effect.mapError((cause) => StepEncodeError.make({ stepName: options.name, cause })),
    );
    yield* stored.complete(encoded);
    return value;
  });

export interface MemoryOptions {
  readonly checkpoints: Map<string, unknown>;
  readonly onStep?: (name: string) => void;
}

/** @internal Creates one in-memory task run over durable test checkpoints. */
export const makeMemory = ({ checkpoints, onStep }: MemoryOptions): BeginStep => {
  const occurrences = new Map<string, number>();
  return (name) =>
    Effect.sync(() => {
      onStep?.(name);
      const occurrence = (occurrences.get(name) ?? 0) + 1;
      occurrences.set(name, occurrence);
      const checkpointName = occurrence === 1 ? name : `${name}#${occurrence}`;
      return {
        value: checkpoints.has(checkpointName)
          ? Option.some(checkpoints.get(checkpointName))
          : Option.none(),
        complete: (value: unknown) =>
          Effect.sync(() => {
            checkpoints.set(checkpointName, value);
          }),
      };
    });
};
