import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layer({ filename: ":memory:" })));

const insertProjectEvent = (input: {
  readonly sequenceHint: number;
  readonly eventId: string;
  readonly projectId: string;
  readonly type: "project.created" | "project.meta-updated";
  readonly payload: Record<string, unknown>;
}) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`
      INSERT INTO orchestration_events (
        event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
        command_id, causation_event_id, correlation_id, actor_kind, payload_json, metadata_json
      ) VALUES (
        ${input.eventId}, 'project', ${input.projectId}, ${input.sequenceHint}, ${input.type},
        '2026-01-01T00:00:00.000Z', NULL, NULL, NULL, 'client',
        ${encodeJson({ projectId: input.projectId, updatedAt: "2026-01-01T00:00:00.000Z", ...input.payload })},
        '{}'
      )
    `;
  });

const readSettings = (projectId: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql<{
      readonly default_thread_env_mode: string | null;
      readonly auto_pull: number;
      readonly favicon_path: string | null;
      readonly project_icon_json: string | null;
    }>`
      SELECT default_thread_env_mode, auto_pull, favicon_path, project_icon_json
      FROM projection_projects
      WHERE project_id = ${projectId}
    `;
    return rows[0];
  });

layer("057_RestoreProjectSettingsFromLegacyEvents", (it) => {
  it.effect("restores settings the 055 baseline reset, and keeps later edits", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 54 });
      const created = { title: "P", workspaceRoot: "/w", defaultModelSelection: null, scripts: [] };
      // Reset: settings live only in V1 events; the row lost them on the first V2 boot.
      yield* insertProjectEvent({
        sequenceHint: 0,
        eventId: "reset:created",
        projectId: "reset",
        type: "project.created",
        payload: { ...created, faviconPath: null, projectIcon: null },
      });
      yield* insertProjectEvent({
        sequenceHint: 1,
        eventId: "reset:settings",
        projectId: "reset",
        type: "project.meta-updated",
        payload: {
          defaultThreadEnvMode: "worktree",
          autoPull: true,
          faviconPath: "brand/icon.svg",
          projectIcon: { kind: "emoji", emoji: "🦊" },
        },
      });
      // Cleared: the user explicitly removed the icon later; 057 must not bring it back.
      yield* insertProjectEvent({
        sequenceHint: 0,
        eventId: "cleared:created",
        projectId: "cleared",
        type: "project.created",
        payload: created,
      });
      yield* insertProjectEvent({
        sequenceHint: 1,
        eventId: "cleared:icon",
        projectId: "cleared",
        type: "project.meta-updated",
        payload: { projectIcon: { kind: "emoji", emoji: "🦊" } },
      });
      yield* insertProjectEvent({
        sequenceHint: 2,
        eventId: "cleared:icon-removed",
        projectId: "cleared",
        type: "project.meta-updated",
        payload: { projectIcon: null },
      });
      for (const projectId of ["reset", "cleared"]) {
        yield* sql`
          INSERT INTO projection_projects (
            project_id, title, workspace_root, scripts_json, created_at, updated_at, deleted_at
          ) VALUES (
            ${projectId}, 'P', ${`/w/${projectId}`}, '[]',
            '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z', NULL
          )
        `;
      }

      yield* runMigrations({ toMigrationInclusive: 56 });
      // 055's baseline carries none of the four settings.
      const baseline = yield* sql<{ readonly payload_json: string }>`
        SELECT payload_json FROM orchestration_events
        WHERE event_id = 'migration:39:project:reset:baseline'
      `;
      assert.notInclude(baseline[0]?.payload_json ?? "", "autoPull");
      assert.deepEqual(yield* readSettings("reset"), {
        default_thread_env_mode: null,
        auto_pull: 0,
        favicon_path: null,
        project_icon_json: null,
      });

      yield* runMigrations({ toMigrationInclusive: 57 });
      assert.deepEqual(yield* readSettings("reset"), {
        default_thread_env_mode: "worktree",
        auto_pull: 1,
        favicon_path: "brand/icon.svg",
        project_icon_json: encodeJson({ kind: "emoji", emoji: "🦊" }),
      });
      assert.deepEqual(yield* readSettings("cleared"), {
        default_thread_env_mode: null,
        auto_pull: 0,
        favicon_path: null,
        project_icon_json: null,
      });
    }),
  );
});

layer("057_RestoreProjectSettingsFromLegacyEvents on a row edited after the reset", (it) => {
  it.effect("does not overwrite a value the user set in V2", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 54 });
      yield* insertProjectEvent({
        sequenceHint: 0,
        eventId: "edited:created",
        projectId: "edited",
        type: "project.created",
        payload: { title: "P", workspaceRoot: "/w", defaultModelSelection: null, scripts: [] },
      });
      yield* insertProjectEvent({
        sequenceHint: 1,
        eventId: "edited:env",
        projectId: "edited",
        type: "project.meta-updated",
        payload: { defaultThreadEnvMode: "worktree" },
      });
      yield* sql`
        INSERT INTO projection_projects (
          project_id, title, workspace_root, scripts_json, created_at, updated_at, deleted_at,
          default_thread_env_mode
        ) VALUES (
          'edited', 'P', '/w', '[]', '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z',
          NULL, 'local'
        )
      `;
      yield* runMigrations({ toMigrationInclusive: 57 });
      assert.equal((yield* readSettings("edited"))?.default_thread_env_mode, "local");
    }),
  );
});
