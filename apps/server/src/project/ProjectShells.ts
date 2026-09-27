/**
 * Project shells read straight from the projected `projection_projects` rows.
 *
 * Shells carry no enrichment unless a caller asks for it: repository identity
 * comes from `ProjectEnrichmentService` and only the immediately available
 * value is used, so a slow git probe never blocks a read.
 */
import type { OrchestrationProjectShell, ProjectId, RepositoryIdentity } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import type {
  ProjectionProject,
  ProjectionProjectRepository,
} from "../persistence/Services/ProjectionProjects.ts";
import type { ProjectEnrichmentService } from "./ProjectEnrichmentService.ts";

type ProjectRows = ProjectionProjectRepository["Service"];
type ProjectEnrichment = ProjectEnrichmentService["Service"];

function toProjectShell(
  row: ProjectionProject,
  repositoryIdentity: RepositoryIdentity | null = null,
): OrchestrationProjectShell {
  return {
    id: row.projectId,
    title: row.title,
    workspaceRoot: row.workspaceRoot,
    repositoryIdentity,
    defaultModelSelection: row.defaultModelSelection,
    defaultThreadEnvMode: row.defaultThreadEnvMode,
    autoPull: row.autoPull,
    faviconPath: row.faviconPath ?? null,
    projectIcon: row.projectIcon ?? null,
    scripts: row.scripts,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  };
}

/** Active projects in creation order, without enrichment. */
export const listActiveProjectShells = (projects: ProjectRows) =>
  projects
    .listAll()
    .pipe(
      Effect.map((rows) =>
        rows.flatMap((row) => (row.deletedAt === null ? [toProjectShell(row)] : [])),
      ),
    );

/** The earliest active project registered at exactly this workspace root. */
export const findActiveProjectByWorkspaceRoot = (projects: ProjectRows, workspaceRoot: string) =>
  projects
    .listAll()
    .pipe(
      Effect.map((rows) =>
        Option.fromNullishOr(
          rows.find((row) => row.deletedAt === null && row.workspaceRoot === workspaceRoot),
        ).pipe(Option.map((row) => toProjectShell(row))),
      ),
    );

const enrich = (enrichment: ProjectEnrichment, row: ProjectionProject) =>
  enrichment
    .getAvailable(row.workspaceRoot)
    .pipe(Effect.map((available) => toProjectShell(row, available.repositoryIdentity)));

/** One active project with its immediately available repository identity. */
export const getActiveProjectShell = (
  projects: ProjectRows,
  enrichment: ProjectEnrichment,
  projectId: ProjectId,
) =>
  projects
    .getById({ projectId })
    .pipe(
      Effect.flatMap((row) =>
        Option.isNone(row) || row.value.deletedAt !== null
          ? Effect.succeed(Option.none<OrchestrationProjectShell>())
          : enrich(enrichment, row.value).pipe(Effect.map(Option.some)),
      ),
    );

/** Active projects, optionally only these IDs, with available repository identities. */
export const listActiveProjectShellsEnriched = (
  projects: ProjectRows,
  enrichment: ProjectEnrichment,
  projectIds?: ReadonlyArray<ProjectId>,
) => {
  const selected = projectIds === undefined ? undefined : new Set(projectIds);
  return projects.listAll().pipe(
    Effect.flatMap((rows) =>
      Effect.forEach(
        rows.filter(
          (row) =>
            row.deletedAt === null && (selected === undefined || selected.has(row.projectId)),
        ),
        (row) => enrich(enrichment, row),
        { concurrency: 16 },
      ),
    ),
  );
};
