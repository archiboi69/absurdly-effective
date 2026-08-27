# Naming adapter-owned persisted workflow metadata

## Recommendation

Do not introduce `StorageProtocol.ts`. Extend the existing internal
`Persistence.ts` module so it owns the complete durable representation of the
workflow adapter:

- workflow payload/result codecs;
- stable checkpoint, header, and event names;
- schemas for values stored under those names; and
- compatibility readers for older representations.

Import it as a namespace when that makes the boundary clearer:

```ts
import * as Persistence from "./Persistence.ts";

Persistence.interruptCheckpointName;
Persistence.parentExecutionHeaderKey;
Persistence.activityCheckpointName(name, attempt);
Persistence.decodeParentExecution(headers);
```

This is the narrowest established term that covers both identifiers and their
encoded values. `Store.ts` remains the database I/O mechanism; `Persistence.ts`
defines what the adapter durably stores.

If that module eventually becomes too large, split only the adapter-owned
control metadata into `persistedMetadata.ts`. Prefer that qualified name over
plain `metadata.ts`: several entries drive execution rather than merely
describe it.

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

This repository already follows the Effect convention:
[`Persistence.ts`](../../sdks/effect/src/unstable/workflow/Persistence.ts) owns
the adapter's workflow and exit codecs. Moving the stable identifiers and their
value schemas there deepens an existing boundary rather than adding a second,
overlapping abstraction.

### Temporal

Temporal uses
[`reserved.ts`](https://github.com/temporalio/sdk-typescript/blob/main/packages/common/src/reserved.ts)
for exactly a reserved prefix, fixed internal names, and rejection of user
collisions. It does **not** place value conversion or compatibility codecs in
that module. Those live under converter/codec modules, while descriptive
command data has the concrete name
[`user-metadata.ts`](https://github.com/temporalio/sdk-typescript/blob/main/packages/common/src/user-metadata.ts).

This supports `reserved*` only for a pure name registry or validator. Once a
module owns persisted representations and compatibility reads, `ReservedNames`
or `ReservedTokens` understates its responsibility.

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
| `Persistence.ts` | **Use** | Established in Effect and already owns this adapter's durable representation. |
| `persistedMetadata.ts` | Good split name | Accurate if adapter-owned control metadata later deserves a separate module. |
| `storageProtocol.ts` | Avoid | Suggests a negotiated or message-level protocol; the concrete boundary is persistence. |
| `reservedNames.ts` / `reservedKeys.ts` | Too narrow | Fits constants and collision checks, but not value schemas or compatibility readers. |
| `reservedTokens.ts` | Avoid | "Token" usually means an opaque capability/identity, not a persisted field name. |
| `metadata.ts` | Too broad | Does not distinguish user metadata from engine-owned durable control state. |
| `schema.ts` | Misleading | A name builder and interpretation rule are not schemas. |
| `encoding.ts` / `codec.ts` | Too narrow | Covers transformations, not stable identifiers and their execution meaning. |
| `conventions.ts` | Wrong scope | Best for a shared ecosystem vocabulary, as in OpenTelemetry. |
| `format.ts` | Too weak | Describes representation but not storage ownership or compatibility. |

## Compatibility rule

The module name is not the compatibility mechanism. Lock compatibility with
literal golden assertions and behavioral tests that seed old persisted names
without importing the current constants. When a representation changes, write
the new form while retaining readers for every form that can still exist in a
live workflow.
