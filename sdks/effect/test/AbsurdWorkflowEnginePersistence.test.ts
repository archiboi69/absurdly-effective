/**
 * Codec laws for the three persistence boundaries of the Absurd-backed
 * `WorkflowEngine`. These run without a database: they pin the value domains
 * that `WorkflowEngine.makeUnsafe` establishes (`unstable/workflow/
 * WorkflowEngine.ts`) so invalid persisted data can only ever defect, never
 * silently degrade into raw values.
 */
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { Workflow } from "effect/unstable/workflow";
import {
  decodeParentExecution,
  decodeStructuralExit,
  encodeStructuralExit,
  exitWithNullishValues,
  makeWorkflowCodecs,
} from "../src/unstable/workflow/AbsurdWorkflowEngine.ts";
// These laws exercise the erased-`Workflow.Any` persistence boundary itself:
// unknown-typed codecs and structural assertions ARE the subject under test,
// mirroring the adapter's own lint exclusions for that seam.
// oxlint-disable anti-slop/no-unknown-parameters
// oxlint-disable anti-slop/no-unknown-returns
// oxlint-disable anti-slop/no-chained-type-assertions
// oxlint-disable anti-slop/require-safety-comment-for-type-assertion
// This suite explicitly verifies Effect's DateFromString Type-side contract.
// oxlint-disable effecttsgo/global-date
import { describe, expect, it } from "@effect/vitest";

class Boom extends Schema.TaggedError<Boom>()("persistence-test/Boom", {
  code: Schema.FiniteFromString,
}) {}

const TransformsWf = Workflow.make("persistence-test/Transforms", {
  payload: { at: Schema.DateFromString },
  success: Schema.Struct({ at: Schema.DateFromString }),
  error: Boom,
  idempotencyKey: ({ at }) => String(at.getTime()),
});

const codecs = makeWorkflowCodecs(TransformsWf);

// These laws exercise service-free schemas; the erased `Workflow.Any` view
// types their channels as `unknown`, so the test casts the same way the
// adapter's `runCodec` boundary does.
const storedEncode =
  (codec: unknown) =>
  (input: unknown): unknown =>
    Schema.encodeSync(codec as unknown as typeof Schema.Any)(input);
const storedDecode =
  (codec: unknown) =>
  (input: unknown): unknown =>
    Schema.decodeUnknownSync(codec as unknown as typeof Schema.Any)(input);
// Feeds a restored encoded-domain exit through the declaration-style decoder
// that `makeUnsafe` uses on this seam.
const handoffDecode = (declaration: unknown, exit: Exit.Exit<unknown, unknown>): unknown =>
  Schema.decodeSync(declaration as unknown as typeof Schema.Any)(exit);

/** The storage medium is jsonb: encoded forms must survive stringify+parse. */
const jsonRoundTrip = (value: unknown): unknown => JSON.parse(JSON.stringify(value)) as unknown;

// Structural views for asserting on decoded values; the codecs return real
// Exit/Cause instances whose narrowing these mirror.
interface FailReasonView {
  readonly _tag: string;
  readonly error?: unknown;
  readonly defect?: unknown;
}
type ExitView =
  | { readonly _tag: "Success"; readonly value: unknown }
  | {
      readonly _tag: "Failure";
      readonly cause: { readonly reasons: ReadonlyArray<FailReasonView> };
    };

const asExitView = (value: unknown): ExitView => value as ExitView;

const failReason = (exit: ExitView, tag: string): FailReasonView | undefined =>
  exit._tag === "Failure" ? exit.cause.reasons.find((reason) => reason._tag === tag) : undefined;

describe("Workflow persistence codecs (Type side ↔ canonical JSON)", () => {
  it("payload: Date transforms survive Type → JSON → Type", () => {
    const stored = storedEncode(codecs.payload)({ at: new Date(0) });
    expect(stored).toEqual({ at: "1970-01-01T00:00:00.000Z" });

    const restored = storedDecode(codecs.payload)(jsonRoundTrip(stored)) as {
      readonly at: Date;
    };
    expect(restored.at).toBeInstanceOf(Date);
    expect(restored.at.getTime()).toBe(0);
  });

  it("result: transformed successes restore as real Dates", () => {
    const complete = new Workflow.Complete({ exit: Exit.succeed({ at: new Date(1250) }) });
    const stored = storedEncode(codecs.result)(complete);
    const restored = asExitView(
      (
        storedDecode(codecs.result)(jsonRoundTrip(stored)) as {
          exit: unknown;
        }
      ).exit,
    );

    expect(restored._tag).toBe("Success");
    const value = restored._tag === "Success" ? (restored.value as { at: Date }) : undefined;
    expect(value?.at).toBeInstanceOf(Date);
    expect(value?.at.getTime()).toBe(1250);
  });

  it("result: tagged errors keep their identity across transformed fields", () => {
    const complete = new Workflow.Complete({ exit: Exit.fail(Boom.make({ code: 42 })) });
    const serialized = JSON.stringify(storedEncode(codecs.result)(complete));
    // NumberFromString's canonical JSON form is a string.
    expect(serialized).toContain('"code":"42"');

    const restored = asExitView(
      (
        storedDecode(codecs.result)(jsonRoundTrip(JSON.parse(serialized))) as {
          exit: unknown;
        }
      ).exit,
    );
    const fail = failReason(restored, "Fail");
    expect((fail?.error as { _tag?: string } | undefined)?._tag).toBe("persistence-test/Boom");
    expect((fail?.error as Boom | undefined)?.code).toBe(42);
  });

  it("result: defects serialize and reconstruct Error instances", () => {
    const complete = new Workflow.Complete({ exit: Exit.die(new TypeError("boom")) });
    const stored = storedEncode(codecs.result)(complete);
    const restored = asExitView(
      (storedDecode(codecs.result)(jsonRoundTrip(stored)) as { exit: unknown }).exit,
    );
    const die = failReason(restored, "Die");
    expect(die?.defect).toBeInstanceOf(Error);
    expect((die?.defect as Error | undefined)?.message).toBe("boom");
  });
});

describe("Workflow format compatibility", () => {
  it("reads the literal parent-execution header", () => {
    // Keep this literal independent of the exported key so the persisted
    // spelling remains part of the compatibility test.
    const parent = Effect.runSync(
      decodeParentExecution({
        "$absurd:effect:v1:parent": {
          queue: "finance",
          executionId: "invoice-123",
        },
      }),
    );

    expect(Option.getOrUndefined(parent)).toEqual({
      queue: "finance",
      executionId: "invoice-123",
    });
  });
});

describe("Activity / deferred structural envelope (already-encoded domain)", () => {
  it("preserves encoded values without applying owning schemas", () => {
    // Activity.make pre-encodes Date successes to ISO strings before the
    // engine sees them; storage must not reapply the transform.
    const iso = "1970-01-01T00:00:00.000Z";
    const restored = Exit.succeed(iso).pipe(
      encodeStructuralExit,
      jsonRoundTrip,
      decodeStructuralExit,
      asExitView,
    );

    expect(restored._tag).toBe("Success");
    const encodedValue =
      restored._tag === "Success"
        ? Schema.decodeUnknownSync(Schema.String)(restored.value)
        : undefined;
    expect(encodedValue).toBe(iso);

    // The handoff contract: makeUnsafe feeds the restored exit through
    // toJsonExit + exitSchemaPartial, which owns the Type-side decode.
    const partial = Schema.Exit(
      Schema.toCodecJson(Schema.DateFromString),
      Schema.toCodecJson(Schema.Never),
      Schema.Unknown,
    );
    const restoredForHandoff = Exit.succeed(iso).pipe(
      encodeStructuralExit,
      jsonRoundTrip,
      decodeStructuralExit,
      exitWithNullishValues,
    );
    const typed = handoffDecode(partial, restoredForHandoff);
    expect((typed as { readonly value?: unknown }).value).toBeInstanceOf(Date);
    expect(((typed as { readonly value?: unknown }).value as Date).getTime()).toBe(0);
  });

  it("reconstructs Error defects instead of losing their identity", () => {
    const restored = Exit.die(new RangeError("gone")).pipe(
      encodeStructuralExit,
      jsonRoundTrip,
      decodeStructuralExit,
      asExitView,
    );
    const die = failReason(restored, "Die");
    // Schema.Defect reconstructs a real Error carrying name+message.
    expect(die?.defect).toBeInstanceOf(Error);
    expect((die?.defect as Error | undefined)?.name).toBe("RangeError");
    expect((die?.defect as Error | undefined)?.message).toBe("gone");
  });

  it("normalizes nullish Void successes for jsonb storage", () => {
    const stored = encodeStructuralExit(Exit.succeed(undefined));
    expect(JSON.stringify(stored)).toContain('"value":null');

    // A Void-success deferred (DurableDeferred.make default) decodes cleanly.
    const voidPartial = Schema.Exit(
      Schema.toCodecJson(Schema.Void),
      Schema.toCodecJson(Schema.Never),
      Schema.toCodecJson(Schema.Defect()),
    );
    const typed = handoffDecode(voidPartial, decodeStructuralExit(jsonRoundTrip(stored)));
    expect((typed as { _tag?: string })._tag).toBe("Success");
  });
});
