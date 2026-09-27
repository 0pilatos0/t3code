import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

interface ProjectEventRow {
  readonly stream_id: string;
  readonly event_type: string;
  readonly payload_json: string;
}

interface ProjectSettingsRow {
  readonly project_id: string;
  readonly default_thread_env_mode: string | null;
  readonly auto_pull: number;
  readonly favicon_path: string | null;
  readonly project_icon_json: string | null;
}

interface ProjectSettings {
  defaultThreadEnvMode: unknown;
  autoPull: unknown;
  faviconPath: unknown;
  projectIcon: unknown;
}

const decodeJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

/**
 * Migration 055 re-baselined every project with a `project.created` event that
 * omitted these four settings, and the first V2 boot replayed it through the
 * V1 projector, which reset them. The settings survive in the project's
 * earlier events, so fold those (skipping the baseline) and restore each
 * column that still holds the reset default. Projects are never rebuilt from
 * the log, so only the row is corrected.
 */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const events = yield* sql<ProjectEventRow>`
    SELECT stream_id, event_type, payload_json
    FROM orchestration_events
    WHERE aggregate_kind = 'project'
      AND event_type IN ('project.created', 'project.meta-updated')
      AND event_id NOT LIKE 'migration:39:project:%'
    ORDER BY sequence ASC
  `;
  if (events.length === 0) return;

  const folded = new Map<string, ProjectSettings>();
  for (const event of events) {
    const payload = yield* decodeJson(event.payload_json);
    if (typeof payload !== "object" || payload === null) continue;
    const values = payload as Record<string, unknown>;
    if (event.event_type === "project.created") {
      // Mirrors the V1 projector: creation never carried env mode or auto-pull.
      folded.set(event.stream_id, {
        defaultThreadEnvMode: null,
        autoPull: false,
        faviconPath: values.faviconPath ?? null,
        projectIcon: values.projectIcon ?? null,
      });
      continue;
    }
    const current = folded.get(event.stream_id);
    if (current === undefined) continue;
    for (const key of ["defaultThreadEnvMode", "autoPull", "faviconPath", "projectIcon"] as const) {
      if (values[key] !== undefined) current[key] = values[key];
    }
  }

  const rows = yield* sql<ProjectSettingsRow>`
    SELECT project_id, default_thread_env_mode, auto_pull, favicon_path, project_icon_json
    FROM projection_projects
  `;
  for (const row of rows) {
    const settings = folded.get(row.project_id);
    if (settings === undefined) continue;
    const defaultThreadEnvMode =
      row.default_thread_env_mode === null && typeof settings.defaultThreadEnvMode === "string"
        ? settings.defaultThreadEnvMode
        : row.default_thread_env_mode;
    const autoPull = row.auto_pull === 0 && settings.autoPull === true ? 1 : row.auto_pull;
    const faviconPath =
      row.favicon_path === null && typeof settings.faviconPath === "string"
        ? settings.faviconPath
        : row.favicon_path;
    const projectIconJson =
      row.project_icon_json === null &&
      typeof settings.projectIcon === "object" &&
      settings.projectIcon !== null
        ? yield* encodeJson(settings.projectIcon)
        : row.project_icon_json;
    if (
      defaultThreadEnvMode === row.default_thread_env_mode &&
      autoPull === row.auto_pull &&
      faviconPath === row.favicon_path &&
      projectIconJson === row.project_icon_json
    ) {
      continue;
    }
    yield* sql`
      UPDATE projection_projects
      SET
        default_thread_env_mode = ${defaultThreadEnvMode},
        auto_pull = ${autoPull},
        favicon_path = ${faviconPath},
        project_icon_json = ${projectIconJson}
      WHERE project_id = ${row.project_id}
    `;
  }
});
