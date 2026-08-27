import * as Context from "effect/Context";
import type * as Schema from "effect/Schema";

export interface CurrentTaskService {
  /** Stable Absurd task UUID. */
  readonly id: string;
  /** Headers attached when the task was spawned. */
  readonly headers: Schema.JsonObject;
}

export class CurrentTask extends Context.Service<CurrentTask, CurrentTaskService>()(
  "absurd-effect/CurrentTask",
) {}
