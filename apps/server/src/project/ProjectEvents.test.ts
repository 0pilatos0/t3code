import { CommandId, EventId, ProjectId, ProviderInstanceId } from "@t3tools/contracts";
import { assert, describe, it } from "@effect/vitest";

import type { ProjectionProject } from "../persistence/Services/ProjectionProjects.ts";
import {
  planProjectEvent,
  projectCommandRejection,
  projectProjectEvent,
  type ProjectCommand,
} from "./ProjectEvents.ts";

const projectId = ProjectId.make("project:events");
const base = (at: string) => ({
  eventId: EventId.make(`event:${at}`),
  commandId: CommandId.make(`command:${at}`),
  occurredAt: at,
  metadata: {},
});
const apply = (row: ProjectionProject | undefined, command: ProjectCommand, at: string) => {
  assert.isNull(projectCommandRejection(command, row));
  return projectProjectEvent(row, planProjectEvent(command, base(at)));
};

const created = apply(
  undefined,
  {
    type: "project.create",
    projectId,
    title: "Events",
    workspaceRoot: "/work/events",
    scripts: [],
  },
  "2026-01-01T00:00:00.000Z",
);

describe("project events", () => {
  it("creates projects without a model default and records one only on update", () => {
    assert.isDefined(created);
    assert.isNull(created?.defaultModelSelection);
    assert.isNull(created?.defaultThreadEnvMode);
    assert.isFalse(created?.autoPull);
    const selection = {
      instanceId: ProviderInstanceId.make("codex"),
      model: "gpt-5.6-sol",
      options: [{ id: "reasoningEffort", value: "high" }],
    };
    const updated = apply(
      created,
      { type: "project.meta.update", projectId, defaultModelSelection: selection },
      "2026-01-02T00:00:00.000Z",
    );
    assert.deepEqual(updated?.defaultModelSelection, selection);
    assert.equal(updated?.updatedAt, "2026-01-02T00:00:00.000Z");
    assert.equal(updated?.createdAt, "2026-01-01T00:00:00.000Z");
  });

  it("leaves omitted fields alone and clears nullable fields on explicit null", () => {
    const set = apply(
      created,
      { type: "project.meta.update", projectId, defaultThreadEnvMode: "worktree", autoPull: true },
      "2026-01-02T00:00:00.000Z",
    );
    const renamed = apply(
      set,
      { type: "project.meta.update", projectId, title: "Renamed" },
      "2026-01-03T00:00:00.000Z",
    );
    assert.equal(renamed?.defaultThreadEnvMode, "worktree");
    assert.isTrue(renamed?.autoPull);
    const cleared = apply(
      renamed,
      { type: "project.meta.update", projectId, defaultThreadEnvMode: null, autoPull: false },
      "2026-01-04T00:00:00.000Z",
    );
    assert.isNull(cleared?.defaultThreadEnvMode);
    assert.isFalse(cleared?.autoPull);
    assert.equal(cleared?.title, "Renamed");
  });

  it("soft-deletes and then rejects further commands for the project", () => {
    const deleted = apply(
      created,
      { type: "project.delete", projectId },
      "2026-01-05T00:00:00.000Z",
    );
    assert.equal(deleted?.deletedAt, "2026-01-05T00:00:00.000Z");
    assert.equal(deleted?.updatedAt, "2026-01-05T00:00:00.000Z");
    assert.isNotNull(
      projectCommandRejection({ type: "project.meta.update", projectId, title: "Late" }, deleted),
    );
    assert.isNotNull(
      projectCommandRejection(
        {
          type: "project.create",
          projectId,
          title: "Again",
          workspaceRoot: "/work/events",
          scripts: [],
        },
        deleted,
      ),
    );
  });
});
