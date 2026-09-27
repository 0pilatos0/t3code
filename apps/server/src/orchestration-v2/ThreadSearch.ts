/**
 * Bounded thread search over user messages and canonical assistant outputs.
 *
 * Covers native V2 threads and legacy V1 threads whose transcripts have not
 * been imported yet (their messages still live in the V1 projection tables).
 */
import {
  IsoDateTime,
  OrchestrationThreadSearchSource,
  ProjectId,
  ThreadId,
  type OrchestrationSearchThreadsInput,
  type OrchestrationSearchThreadsResult,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as SqlSchema from "effect/unstable/sql/SqlSchema";

import { toPersistenceDecodeError, toPersistenceSqlError } from "../persistence/Errors.ts";

const ProjectionThreadSearchRequest = Schema.Struct({
  pattern: Schema.String,
  limit: Schema.Int,
});
const ProjectionThreadSearchRow = Schema.Struct({
  threadId: ThreadId,
  projectId: ProjectId,
  source: OrchestrationThreadSearchSource,
  matchText: Schema.String,
  messageCreatedAt: Schema.NullOr(IsoDateTime),
});

function escapeLikePattern(value: string): string {
  return value.replaceAll("!", "!!").replaceAll("%", "!%").replaceAll("_", "!_");
}

function foldAsciiCase(value: string): string {
  return value.replace(/[A-Z]/g, (character) => character.toLowerCase());
}

function buildSearchSnippet(text: string, query: string): string {
  const normalizedText = text.replace(/\s+/g, " ").trim();
  if (normalizedText.length <= 240) {
    return normalizedText;
  }

  const normalizedQuery = foldAsciiCase(query.replace(/\s+/g, " ").trim());
  const matchIndex = foldAsciiCase(normalizedText).indexOf(normalizedQuery);
  const bodyLength = 236;
  const idealStart = Math.max(0, matchIndex - 72);
  const start = Math.min(idealStart, normalizedText.length - bodyLength);
  const end = Math.min(normalizedText.length, start + bodyLength);
  return `${start > 0 ? "…" : ""}${normalizedText.slice(start, end)}${
    end < normalizedText.length ? "…" : ""
  }`;
}

export const searchThreads = Effect.fn("ThreadSearch.searchThreads")(function* (
  input: OrchestrationSearchThreadsInput,
) {
  const sql = yield* SqlClient.SqlClient;
  const searchActiveThreadRows = SqlSchema.findAll({
    Request: ProjectionThreadSearchRequest,
    Result: ProjectionThreadSearchRow,
    execute: ({ pattern, limit }) =>
      sql`
        WITH candidate AS (
          SELECT
            threads.thread_id AS thread_id,
            COALESCE(v2_threads.project_id, threads.project_id) AS project_id,
            messages.role AS role,
            messages.text AS match_text,
            messages.created_at AS message_created_at,
            messages.message_id AS message_id,
            COALESCE(v2_threads.updated_at, threads.updated_at) AS thread_updated_at
          FROM projection_thread_messages AS messages
          INNER JOIN projection_threads AS threads
            ON threads.thread_id = messages.thread_id
          LEFT JOIN orchestration_v2_projection_threads AS v2_threads
            ON v2_threads.thread_id = threads.thread_id
          INNER JOIN projection_projects AS projects
            ON projects.project_id = COALESCE(v2_threads.project_id, threads.project_id)
          WHERE (
              v2_threads.thread_id IS NULL
              AND threads.deleted_at IS NULL
              AND threads.archived_at IS NULL
            OR
              v2_threads.thread_id IS NOT NULL
              AND v2_threads.deleted_at IS NULL
              AND v2_threads.archived_at IS NULL
            )
            AND projects.deleted_at IS NULL
            AND messages.is_streaming = 0
            AND (
              messages.role = 'user'
              OR (
                messages.role = 'assistant'
                AND messages.message_id IN (
                  SELECT turns.assistant_message_id
                  FROM projection_turns AS turns
                  WHERE turns.assistant_message_id IS NOT NULL
                )
              )
            )
            AND messages.text LIKE ${pattern} ESCAPE '!'
          UNION ALL
          SELECT
            v2_threads.thread_id AS thread_id,
            v2_threads.project_id AS project_id,
            v2_messages.role AS role,
            json_extract(v2_messages.payload_json, '$.text') AS match_text,
            v2_messages.created_at AS message_created_at,
            v2_messages.message_id AS message_id,
            v2_threads.updated_at AS thread_updated_at
          FROM orchestration_v2_projection_messages AS v2_messages
          INNER JOIN orchestration_v2_projection_threads AS v2_threads
            ON v2_threads.thread_id = v2_messages.thread_id
          INNER JOIN projection_projects AS projects
            ON projects.project_id = v2_threads.project_id
          WHERE v2_threads.deleted_at IS NULL
            AND v2_threads.archived_at IS NULL
            AND projects.deleted_at IS NULL
            AND v2_messages.streaming = 0
            AND v2_messages.role IN ('user', 'assistant')
            AND json_extract(v2_messages.payload_json, '$.text') LIKE ${pattern} ESCAPE '!'
        ),
        ranked AS (
          SELECT
            thread_id,
            project_id,
            CASE role
              WHEN 'user' THEN 'user'
              ELSE 'assistant'
            END AS source,
            match_text,
            message_created_at,
            CASE role
              WHEN 'user' THEN 0
              ELSE 1
            END AS match_rank,
            thread_updated_at,
            ROW_NUMBER() OVER (
              PARTITION BY thread_id
              ORDER BY
                CASE role
                  WHEN 'user' THEN 0
                  ELSE 1
                END ASC,
                message_created_at DESC,
                message_id ASC
            ) AS thread_match_rank
          FROM candidate
        )
        SELECT
          thread_id AS "threadId",
          project_id AS "projectId",
          source,
          match_text AS "matchText",
          message_created_at AS "messageCreatedAt"
        FROM ranked
        WHERE thread_match_rank = 1
        ORDER BY
          match_rank ASC,
          thread_updated_at DESC,
          thread_id ASC
        LIMIT ${limit}
      `,
  });
  const rows = yield* searchActiveThreadRows({
    pattern: `%${escapeLikePattern(input.query)}%`,
    limit: input.limit ?? 50,
  }).pipe(
    Effect.mapError((cause) =>
      Schema.isSchemaError(cause)
        ? toPersistenceDecodeError("ThreadSearch.searchThreads:decodeRows")(cause)
        : toPersistenceSqlError("ThreadSearch.searchThreads:query")(cause),
    ),
  );
  return {
    matches: rows.map((row) => ({
      threadId: row.threadId,
      projectId: row.projectId,
      source: row.source,
      snippet: buildSearchSnippet(row.matchText, input.query),
      messageCreatedAt: row.messageCreatedAt,
    })),
  } satisfies OrchestrationSearchThreadsResult;
});
