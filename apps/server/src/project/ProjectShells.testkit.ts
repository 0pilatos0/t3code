import type { OrchestrationProjectShell } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import {
  type ProjectionProject,
  ProjectionProjectRepository,
} from "../persistence/Services/ProjectionProjects.ts";
import { ProjectEnrichmentService } from "./ProjectEnrichmentService.ts";

function toRow(shell: OrchestrationProjectShell): ProjectionProject {
  return {
    projectId: shell.id,
    title: shell.title,
    workspaceRoot: shell.workspaceRoot,
    defaultModelSelection: shell.defaultModelSelection,
    defaultThreadEnvMode: shell.defaultThreadEnvMode ?? null,
    autoPull: shell.autoPull ?? false,
    faviconPath: shell.faviconPath ?? null,
    projectIcon: shell.projectIcon ?? null,
    scripts: shell.scripts,
    createdAt: shell.createdAt,
    updatedAt: shell.updatedAt,
    deletedAt: null,
  };
}

/**
 * Serves these active project shells as projected rows, and their repository
 * identities as already-resolved enrichment.
 */
export const projectShellsTestLayer = (shells: ReadonlyArray<OrchestrationProjectShell>) => {
  const rows = shells.map(toRow);
  const identityByRoot = new Map(
    shells.map((shell) => [shell.workspaceRoot, shell.repositoryIdentity ?? null]),
  );
  const enrichmentOf = (workspaceRoot: string) =>
    Effect.succeed({
      repositoryIdentity: identityByRoot.get(workspaceRoot) ?? null,
      faviconPath: null,
      repositoryIdentityResolved: true,
    });
  return Layer.mergeAll(
    Layer.mock(ProjectionProjectRepository)({
      listAll: () => Effect.succeed(rows),
      getById: ({ projectId }) =>
        Effect.succeed(Option.fromNullishOr(rows.find((row) => row.projectId === projectId))),
    }),
    Layer.mock(ProjectEnrichmentService)({
      peek: enrichmentOf,
      getAvailable: enrichmentOf,
    }),
  );
};
