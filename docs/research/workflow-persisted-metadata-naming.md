# Naming adapter-owned persisted workflow metadata

## Recommendation

Do not introduce `StorageProtocol.ts`. Keep the workflow adapter's durable
contract at its two actual owners:

- `Reserved.ts` owns stable checkpoint, header, and event identifiers; and
- `AbsurdWorkflowEngine.ts` owns its private schemas, codecs, canonical stored
  values, and compatibility readers.

Import the identifier registry as a namespace. Keep value conversion local to
the engine that consumes it:

```ts
import * as Reserved from "./Reserved.ts";

Reserved.interruptCheckpointName;
Reserved.parentExecutionHeaderKey;
Reserved.activityCheckpointName(name, attempt);
decodeParentExecution(headers);
encodeStructuralExit(exit);
```

These two owners jointly define the workflow persistence contract.
`internal/AbsurdSql.ts` remains the database I/O mechanism: it says how the SDK
talks to PostgreSQL. `Reserved.ts` says where engine state is stored, while the
engine's helpers say how its values cross the durable JSON boundary. The
helpers have no independent consumer, configuration, state, or lifecycle, so a
separate production module would be organization without encapsulation.

## Evidence

### Effect

Effect names the durable boundary `Persistence`: its `Persistence` service
stores schema-encoded exits that survive fibers, processes, and workers. The
adjacent `Persistable` and `SchemaStore` names describe specific roles rather
than a general "protocol." See
[`Persistence.ts`](../../.context/effect/packages/effect/src/unstable/persistence/Persistence.ts)
and
[`KeyValueStore.ts`](../../.context/effect/packages/effect/src/unstable/persistence/KeyValueStore.ts).

The closest workflow implementation does not introduce a `Protocol` or
`ReservedTokens` abstraction. `ClusterWorkflowEngine` keeps the stable
`payloadParentKey`, its value schema, persisted RPC definitions, and the
`activityPrimaryKey` builder together at the persistence boundary. See
[`ClusterWorkflowEngine.ts`](../../.context/effect/packages/effect/src/unstable/cluster/ClusterWorkflowEngine.ts).
Its genuinely message-shaped concepts instead receive concrete names such as
`Envelope`, `Message`, and `Reply`.

The Effect name establishes that these concerns collectively form a persistence
boundary. It does not require one source file to own every part of that
boundary. In this adapter, keeping identifiers and representations separate
makes the engine call sites more precise without introducing new services or
public concepts.

### Temporal

Temporal uses
[`reserved.ts`](https://github.com/temporalio/sdk-typescript/blob/main/packages/common/src/reserved.ts)
for exactly a reserved prefix, fixed internal names, and rejection of user
collisions. It does **not** place value conversion or compatibility codecs in
that module. Those live under converter/codec modules, while descriptive
command data has the concrete name
[`user-metadata.ts`](https://github.com/temporalio/sdk-typescript/blob/main/packages/common/src/user-metadata.ts).

This supports `Reserved.ts` for the pure name registry and keeping
representations beside their sole engine consumer. The split preserves
Temporal's useful distinction without importing its larger converter
architecture.

### Kubernetes

Kubernetes calls externally visible key/value extensions *annotations*. It
requires automated components to namespace their annotation keys and reserves
the `kubernetes.io/` and `k8s.io/` prefixes for core components. The canonical
catalog is named
[*Well-Known Labels, Annotations and Taints*](https://kubernetes.io/docs/reference/labels-annotations-taints/),
not a protocol or codec. The syntax and ownership rules are documented in
[*Annotations*](https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/).

`Annotations` would be appropriate if Absurd exposed one generic metadata map
with that vocabulary. Here the identifiers span headers, checkpoints, and
events, so the Kubernetes name does not transfer cleanly. Its useful lesson is
to namespace stable names by their owning component.

### OpenTelemetry

OpenTelemetry calls a shared catalog of names, types, meanings, and valid values
[*Semantic Conventions*](https://opentelemetry.io/docs/specs/semconv/). It uses
namespaces for those attributes and versions the conventions independently;
generated libraries use the `opentelemetry-semconv` artifact and namespace.
See the official
[*semantic convention artifact structure*](https://opentelemetry.io/docs/specs/semconv/non-normative/code-generation/).

`Conventions` fits a cross-library vocabulary intended for many independent
producers and consumers. The Absurd Effect identifiers are a private durable
representation interpreted by one adapter, so `WorkflowConventions` would make
their scope sound broader and less binding than it is.

## Candidate verdicts

| Name | Verdict | Reason |
| --- | --- | --- |
| `Reserved.ts` | **Use for identifiers** | Precisely owns fixed engine names and builders in the reserved namespace. |
| Engine-local serialization helpers | **Use for values** | The vocabulary is accurate, but the helpers do not form an independent module boundary. |
| `Persistence.ts` | Accurate umbrella, avoid as a file | Names the combined boundary but makes individual call sites less precise. |
| `persistedMetadata.ts` | Avoid | Longer and less exact than separating reserved addresses from value formats. |
| `storageProtocol.ts` | Avoid | Suggests a negotiated or message-level protocol; the concrete boundary is persistence. |
| `reservedNames.ts` / `reservedKeys.ts` | Needlessly qualified | The namespace import already supplies the noun: `Reserved.parentExecutionHeaderKey`. |
| `reservedTokens.ts` | Avoid | "Token" usually means an opaque capability/identity, not a persisted field name. |
| `metadata.ts` | Too broad | Does not distinguish user metadata from engine-owned durable control state. |
| `schema.ts` | Misleading | A name builder and interpretation rule are not schemas. |
| `encoding.ts` / `codec.ts` | Too narrow | Covers transformations, not stable identifiers and their execution meaning. |
| `conventions.ts` | Wrong scope | Best for a shared ecosystem vocabulary, as in OpenTelemetry. |
| `serialization.ts` | Premature module | Accurate vocabulary, but there is only one consumer and no independent abstraction. |
| `format.ts` | Too weak | Describes the representation but not the encode/decode boundary that owns it. |

## Compatibility rule

The module split is not the compatibility mechanism. Lock names with literal
golden assertions and lock formats with round-trip and compatibility tests that
seed old stored values without importing the current builders. When either
side changes, write the new form while retaining readers for every form that
can still exist in a live workflow.
