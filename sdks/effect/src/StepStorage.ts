import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import {
  StepDecodeError,
  StepEncodeError,
  type Options,
  type StepPersistenceError,
} from "./Step.ts";
import type { currentTaskStep } from "./CurrentTask.ts";

// Stored step values cross an untyped JSON boundary owned by the success Schema.
// oxlint-disable anti-slop/no-unknown-parameters

export interface StoredStep {
  readonly value: Option.Option<unknown>;
  readonly complete: (value: unknown) => Effect.Effect<void, StepPersistenceError>;
}

export type BeginStep = (name: string) => Effect.Effect<StoredStep, StepPersistenceError>;

export const fromStorage = (begin: BeginStep): ReturnType<typeof currentTaskStep> =>
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
