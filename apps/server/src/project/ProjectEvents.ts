/**
 * Project commands as application events.
 *
 * `planProjectEvent` validates a project command against the current row and
 * returns the one event it produces; `projectProjectEvent` folds a committed
 * event onto the `projection_projects` row. The event types and payloads are
 * the ones already in every user's log (V1 engine, migration 055 baseline), so
 * old and new rows decode alike.
 */
import {
  type ApplicationProjectEvent,
  MAX_SCRIPT_ID_LENGTH,
  type ModelSelection,
  type ProjectIconOverride,
  type ProjectId,
  type ProjectScript,
  SCRIPT_RUN_COMMAND_PATTERN,
  type ThreadEnvMode,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

import type { ProjectionProject } from "../persistence/Services/ProjectionProjects.ts";

export type PlannedProjectEvent = ApplicationProjectEvent extends infer Event
  ? Event extends ApplicationProjectEvent
    ? Omit<Event, "sequence">
    : never
  : never;

type PlannedProjectEventBase = Pick<
  ApplicationProjectEvent,
  "eventId" | "commandId" | "occurredAt" | "metadata"
>;

export type ProjectCommand =
  | {
      readonly type: "project.create";
      readonly projectId: ProjectId;
      readonly title: string;
      readonly workspaceRoot: string;
      readonly scripts: ReadonlyArray<ProjectScript>;
    }
  | {
      readonly type: "project.meta.update";
      readonly projectId: ProjectId;
      readonly title?: string;
      readonly workspaceRoot?: string;
      readonly defaultModelSelection?: ModelSelection | null;
      readonly defaultThreadEnvMode?: ThreadEnvMode | null;
      readonly autoPull?: boolean;
      readonly faviconPath?: string | null;
      readonly projectIcon?: ProjectIconOverride | null;
      readonly scripts?: ReadonlyArray<ProjectScript>;
    }
  | { readonly type: "project.delete"; readonly projectId: ProjectId };

const monogramSegmenter = new Intl.Segmenter(undefined, { granularity: "grapheme" });
const isScriptRunCommand = Schema.is(SCRIPT_RUN_COMMAND_PATTERN);

/** Rejects a new script ID or monogram the client could not have produced itself. */
export function projectCommandRejection(
  command: ProjectCommand,
  existing: ProjectionProject | undefined,
): string | null {
  if (command.type === "project.create") {
    return existing === undefined ? null : `Project '${command.projectId}' already exists.`;
  }
  if (existing === undefined || existing.deletedAt !== null) {
    return `Project '${command.projectId}' does not exist.`;
  }
  if (command.type === "project.delete") return null;
  if (
    command.projectIcon?.kind === "monogram" &&
    Array.from(monogramSegmenter.segment(command.projectIcon.text)).length > 2
  ) {
    return "Project monograms must contain at most two characters.";
  }
  if (command.scripts !== undefined) {
    // Persisted IDs predate shortcut validation. Let users edit or remove them
    // without allowing another invalid ID to enter the project.
    const existingIds = new Set(existing.scripts.map((script) => script.id));
    for (const script of command.scripts) {
      if (!existingIds.has(script.id) && !isScriptRunCommand(`script.${script.id}.run`)) {
        return `Script ID '${script.id}' must be 1-${MAX_SCRIPT_ID_LENGTH} lowercase letters, digits or hyphens, starting with a letter or digit.`;
      }
    }
  }
  return null;
}

/** The event a validated project command produces. */
export function planProjectEvent(
  command: ProjectCommand,
  base: PlannedProjectEventBase,
): PlannedProjectEvent {
  const envelope = {
    ...base,
    aggregateKind: "project" as const,
    aggregateId: command.projectId,
    causationEventId: null,
    correlationId: base.commandId,
  };
  switch (command.type) {
    case "project.create":
      return {
        ...envelope,
        type: "project.created",
        payload: {
          projectId: command.projectId,
          title: command.title,
          workspaceRoot: command.workspaceRoot,
          // Creation has no user model choice; only a metadata update records
          // an explicit project default.
          defaultModelSelection: null,
          faviconPath: null,
          projectIcon: null,
          scripts: command.scripts,
          createdAt: base.occurredAt,
          updatedAt: base.occurredAt,
        },
      };
    case "project.meta.update":
      return {
        ...envelope,
        type: "project.meta-updated",
        payload: {
          projectId: command.projectId,
          ...(command.title === undefined ? {} : { title: command.title }),
          ...(command.workspaceRoot === undefined ? {} : { workspaceRoot: command.workspaceRoot }),
          ...(command.defaultModelSelection === undefined
            ? {}
            : { defaultModelSelection: command.defaultModelSelection }),
          ...(command.defaultThreadEnvMode === undefined
            ? {}
            : { defaultThreadEnvMode: command.defaultThreadEnvMode }),
          ...(command.autoPull === undefined ? {} : { autoPull: command.autoPull }),
          ...(command.faviconPath === undefined ? {} : { faviconPath: command.faviconPath }),
          ...(command.projectIcon === undefined ? {} : { projectIcon: command.projectIcon }),
          ...(command.scripts === undefined ? {} : { scripts: command.scripts }),
          updatedAt: base.occurredAt,
        },
      };
    case "project.delete":
      return {
        ...envelope,
        type: "project.deleted",
        payload: { projectId: command.projectId, deletedAt: base.occurredAt },
      };
  }
}

/** Folds one committed project event onto its projected row. */
export function projectProjectEvent(
  row: ProjectionProject | undefined,
  event: PlannedProjectEvent,
): ProjectionProject | undefined {
  switch (event.type) {
    case "project.created":
      return {
        projectId: event.payload.projectId,
        title: event.payload.title,
        workspaceRoot: event.payload.workspaceRoot,
        defaultModelSelection: event.payload.defaultModelSelection,
        defaultThreadEnvMode: event.payload.defaultThreadEnvMode ?? null,
        autoPull: false,
        faviconPath: event.payload.faviconPath ?? null,
        projectIcon: event.payload.projectIcon ?? null,
        scripts: event.payload.scripts,
        createdAt: event.payload.createdAt,
        updatedAt: event.payload.updatedAt,
        deletedAt: null,
      };
    case "project.meta-updated": {
      if (row === undefined) return undefined;
      const payload = event.payload;
      return {
        ...row,
        ...(payload.title === undefined ? {} : { title: payload.title }),
        ...(payload.workspaceRoot === undefined ? {} : { workspaceRoot: payload.workspaceRoot }),
        ...(payload.defaultModelSelection === undefined
          ? {}
          : { defaultModelSelection: payload.defaultModelSelection }),
        ...(payload.defaultThreadEnvMode === undefined
          ? {}
          : { defaultThreadEnvMode: payload.defaultThreadEnvMode }),
        ...(payload.autoPull === undefined ? {} : { autoPull: payload.autoPull }),
        ...(payload.faviconPath === undefined ? {} : { faviconPath: payload.faviconPath }),
        ...(payload.projectIcon === undefined ? {} : { projectIcon: payload.projectIcon }),
        ...(payload.scripts === undefined ? {} : { scripts: payload.scripts }),
        updatedAt: payload.updatedAt,
      };
    }
    case "project.deleted":
      return row === undefined
        ? undefined
        : { ...row, deletedAt: event.payload.deletedAt, updatedAt: event.payload.deletedAt };
  }
}
