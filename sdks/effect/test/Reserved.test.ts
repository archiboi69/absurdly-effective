import { describe, expect, it } from "@effect/vitest";

import * as Reserved from "../src/unstable/workflow/Reserved.ts";

describe("Reserved workflow identifiers", () => {
  it("keeps every durable spelling stable", () => {
    expect(Reserved.interruptCheckpointName).toBe("$absurd:effect:v1:interrupt");
    expect(Reserved.parentExecutionHeaderKey).toBe("$absurd:effect:v1:parent");
    expect(Reserved.activityCheckpointName("charge", 2)).toBe(
      "$absurd:effect:v1:activity:charge:2",
    );
    expect(Reserved.deferredCheckpointName("approval")).toBe("$absurd:effect:v1:deferred:approval");
    expect(Reserved.clockDeadlineCheckpointName("deadline")).toBe(
      "$absurd:effect:v1:clock:deadline",
    );
    expect(Reserved.doorbellNames("execution-1", "doorbell-1")).toEqual({
      checkpointName: "$absurd:effect:v1:wake:doorbell-1",
      eventName: "$absurd:effect:v1:wake-event:execution-1:doorbell-1",
    });
    expect(Reserved.deferredEventName("Invoice", "execution-1", "approval")).toBe(
      "$absurd:effect:v1:deferred-event:Invoice:execution-1:approval",
    );
  });
});
