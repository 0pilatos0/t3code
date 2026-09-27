/**
 * `t3 thread` - list, start, read, and message threads on the local running
 * server.
 *
 * Uses the same HTTP API as `t3 project`, with a session that only has the
 * orchestration scopes and is revoked on exit. There is no offline mode: a
 * turn needs the running server's provider sessions. `start` sends the same
 * bootstrap turn start as the app's composer, so worktrees and setup scripts
 * follow the project's settings.
 */
import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  DEFAULT_SERVER_SETTINGS,
  EnvironmentHttpApi,
  MessageId,
  type ClientOrchestrationCommand,
  type ModelSelection,
  type OrchestrationLatestTurnState,
  type ThreadEnvMode,
  type OrchestrationMessage,
  OrchestrationMessageRole,
  type OrchestrationProjectShell,
  OrchestrationSessionStatus,
  type OrchestrationThreadShell,
  type ServerProvider,
  type ServerProviderModel,
  ServerSettings,
  ThreadId,
} from "@t3tools/contracts";
import { buildTemporaryWorktreeBranchName } from "@t3tools/shared/git";
import { normalizeProjectPathForComparison } from "@t3tools/shared/path";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import { fromLenientJson } from "@t3tools/shared/schemaJson";
import { isModelSelectionProviderEnabled } from "@t3tools/shared/serverSettings";
import { truncate } from "@t3tools/shared/String";
import * as Console from "effect/Console";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as References from "effect/References";
import * as Runtime from "effect/Runtime";
import * as Schema from "effect/Schema";
import * as Stdio from "effect/Stdio";
import * as Stream from "effect/Stream";
import { Argument, Command, Flag, GlobalFlag } from "effect/unstable/cli";
import { FetchHttpClient } from "effect/unstable/http";
import * as HttpApiClient from "effect/unstable/httpapi/HttpApiClient";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as ServerConfig from "../config.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as T3ProjectFileLoader from "../project/T3ProjectFileLoader.ts";
import { readProviderStatusCache } from "../provider/providerStatusCache.ts";
import { isProcessAlive, readPersistedServerRuntimeState } from "../serverRuntimeState.ts";
import { projectLocationFlags, resolveCliAuthConfig } from "./config.ts";
import { type ProjectCommandError, projectCommandErrorFromLiveServerRequest } from "./project.ts";

const THREAD_CLI_REQUEST_TIMEOUT = Duration.seconds(10);
const THREAD_POLL_INTERVAL = Duration.seconds(2);

export class ThreadServerNotRunningError extends Schema.TaggedError<ThreadServerNotRunningError>()(
  "ThreadServerNotRunningError",
  {},
) {
  override get message(): string {
    return "T3 Code is not running. Open the desktop app or run `t3`.";
  }
}

export class ThreadNotFoundError extends Schema.TaggedError<ThreadNotFoundError>()(
  "ThreadNotFoundError",
  { threadId: Schema.String },
) {
  override get message(): string {
    return `Thread '${this.threadId}' not found.`;
  }
}

export class ThreadProjectNotFoundError extends Schema.TaggedError<ThreadProjectNotFoundError>()(
  "ThreadProjectNotFoundError",
  { project: Schema.String },
) {
  override get message(): string {
    return `Project '${this.project}' not found.`;
  }
}

export class ThreadModelRequiredError extends Schema.TaggedError<ThreadModelRequiredError>()(
  "ThreadModelRequiredError",
  {},
) {
  override get message(): string {
    return "No default model to use. Pass --model.";
  }
}

export class ThreadModelNotFoundError extends Schema.TaggedError<ThreadModelNotFoundError>()(
  "ThreadModelNotFoundError",
  { model: Schema.NullOr(Schema.String), provider: Schema.NullOr(Schema.String) },
) {
  override get message(): string {
    const model = this.model === null ? "A model" : `Model '${this.model}'`;
    const provider = this.provider === null ? "an enabled provider" : `'${this.provider}'`;
    return `${model} was not found on ${provider}.`;
  }
}

export class ThreadModelAmbiguousError extends Schema.TaggedError<ThreadModelAmbiguousError>()(
  "ThreadModelAmbiguousError",
  { model: Schema.String, providers: Schema.Array(Schema.String) },
) {
  override get message(): string {
    return `Model '${this.model}' is on ${this.providers.join(", ")}. Pass --provider.`;
  }
}

export class ThreadEnvModeConflictError extends Schema.TaggedError<ThreadEnvModeConflictError>()(
  "ThreadEnvModeConflictError",
  {},
) {
  override get message(): string {
    return "Pass --worktree or --local, not both.";
  }
}

export class ThreadBaseBranchRequiredError extends Schema.TaggedError<ThreadBaseBranchRequiredError>()(
  "ThreadBaseBranchRequiredError",
  {},
) {
  override get message(): string {
    return "The project checkout is not on a branch. Pass --base <branch> or --local.";
  }
}

export class ThreadWorktreeUnavailableError extends Schema.TaggedError<ThreadWorktreeUnavailableError>()(
  "ThreadWorktreeUnavailableError",
  {},
) {
  override get message(): string {
    return "The project folder is not a git repository, so it cannot have a worktree.";
  }
}

export class ThreadPromptEmptyError extends Schema.TaggedError<ThreadPromptEmptyError>()(
  "ThreadPromptEmptyError",
  {},
) {
  override get message(): string {
    return "Message is empty.";
  }
}

/** Exits 2 when the thread needs the user, so a script can tell that from a failure. */
export class ThreadTurnEndedError extends Schema.TaggedError<ThreadTurnEndedError>()(
  "ThreadTurnEndedError",
  {
    outcome: Schema.Literals(["needs-input", "interrupted", "error"]),
    detail: Schema.NullOr(Schema.String),
  },
) {
  override get [Runtime.errorExitCode](): number {
    return this.outcome === "needs-input" ? 2 : 1;
  }

  override get message(): string {
    switch (this.outcome) {
      case "needs-input":
        return "Thread is waiting on an approval or answer. Resolve it in T3 Code.";
      case "interrupted":
        return "The turn was interrupted.";
      case "error":
        return this.detail === null ? "The turn failed." : `The turn failed: ${this.detail}`;
    }
  }
}

const ThreadStatus = Schema.Union([OrchestrationSessionStatus, Schema.Literal("needs-input")]);

const encodeThreadList = Schema.encodeEffect(
  Schema.fromJsonString(
    Schema.Array(
      Schema.Struct({
        id: ThreadId,
        title: Schema.String,
        project: Schema.String,
        status: ThreadStatus,
        lastActivityAt: Schema.String,
      }),
    ),
    { space: 2 },
  ),
);

const encodeThreadShow = Schema.encodeEffect(
  Schema.fromJsonString(
    Schema.Struct({
      id: ThreadId,
      title: Schema.String,
      status: OrchestrationSessionStatus,
      messages: Schema.Array(
        Schema.Struct({
          id: MessageId,
          role: OrchestrationMessageRole,
          text: Schema.String,
          createdAt: Schema.String,
        }),
      ),
    }),
    { space: 2 },
  ),
);

const encodeTurnResult = Schema.encodeEffect(
  Schema.fromJsonString(
    Schema.Struct({
      threadId: ThreadId,
      branch: Schema.NullOr(Schema.String),
      worktreePath: Schema.NullOr(Schema.String),
      reply: Schema.optionalKey(Schema.String),
    }),
    { space: 2 },
  ),
);

const threadStatus = (thread: OrchestrationThreadShell): typeof ThreadStatus.Type =>
  thread.hasPendingApprovals || thread.hasPendingUserInput
    ? "needs-input"
    : (thread.session?.status ?? "idle");

const isThreadBusy = (thread: OrchestrationThreadShell) =>
  thread.session?.status === "running" || thread.session?.status === "starting";

const lastActivityAt = (thread: OrchestrationThreadShell) =>
  thread.latestUserMessageAt ?? thread.createdAt;

const byLastActivity = (a: OrchestrationThreadShell, b: OrchestrationThreadShell) =>
  lastActivityAt(b).localeCompare(lastActivityAt(a));

/**
 * Finds the model selections that match `--model` and `--provider`. A model
 * matches by slug or alias. With only a provider, it gives that provider's
 * default model.
 */
export const findModelOffers = (
  providers: ReadonlyArray<
    Pick<ServerProvider, "instanceId"> & {
      readonly models: ReadonlyArray<Pick<ServerProviderModel, "slug" | "aliases" | "isDefault">>;
    }
  >,
  model: string | undefined,
  provider: string | undefined,
): ReadonlyArray<ModelSelection> =>
  providers
    .filter((candidate) => provider === undefined || candidate.instanceId === provider)
    .flatMap((candidate) => {
      const entry =
        model === undefined
          ? (candidate.models.find((entry) => entry.isDefault === true) ?? candidate.models[0])
          : candidate.models.find(
              (entry) => entry.slug === model || entry.aliases?.includes(model) === true,
            );
      return entry === undefined ? [] : [{ instanceId: candidate.instanceId, model: entry.slug }];
    });

/**
 * How the message sent at `sentAt` was handled, or undefined while it may
 * still run. `sentAt` is the message's server `createdAt`, which the turn it
 * starts copies into `latestTurn.requestedAt`. The server holds the session
 * at "starting" while a turn start is pending (ProviderCommandReactor and
 * ProviderRuntimeIngestion both keep it there), so an idle session updated
 * after `sentAt` means the message was handled without a turn of its own: a
 * failed start, a provider command, or a steer into another client's turn.
 */
export const turnOutcome = (
  thread: OrchestrationThreadShell,
  sentAt: string,
): Exclude<OrchestrationLatestTurnState, "running"> | "needs-input" | undefined => {
  if (thread.hasPendingApprovals || thread.hasPendingUserInput) return "needs-input";
  if (isThreadBusy(thread)) return undefined;
  const turn = thread.latestTurn;
  if (turn?.requestedAt === sentAt) {
    return turn.state === "running" ? undefined : turn.state;
  }
  const since = Date.parse(sentAt);
  // A later turn only starts after this message's turn has ended.
  if (turn !== null && Date.parse(turn.requestedAt) > since) return "completed";
  const session = thread.session;
  if (session !== null && Date.parse(session.updatedAt) >= since) {
    return session.status === "error" || session.status === "interrupted"
      ? session.status
      : "completed";
  }
  return undefined;
};

const threadCliUuid = Crypto.Crypto.pipe(
  Effect.flatMap((crypto) => crypto.randomUUIDv4),
  Effect.orDie,
);

/** Reads the message argument, or stdin when it is `-`. */
const readMessage = Effect.fn("readThreadMessage")(function* (message: string) {
  const text =
    message === "-"
      ? yield* Stdio.Stdio.pipe(
          Effect.flatMap((stdio) => stdio.stdin.pipe(Stream.decodeText(), Stream.mkString)),
        )
      : message;
  const trimmed = text.trim();
  if (trimmed.length === 0) {
    return yield* new ThreadPromptEmptyError();
  }
  return trimmed;
});

const isPathWithin = (root: string, target: string) =>
  target === root || target.startsWith(`${root}/`) || target.startsWith(`${root}\\`);

/**
 * Matches a project id, or a path inside a project folder or inside one of
 * its threads' worktrees. An agent running in a T3 worktree can pass `.`. The
 * deepest match wins, so a nested project beats its parent.
 */
export const matchProject = (
  snapshot: {
    readonly projects: ReadonlyArray<OrchestrationProjectShell>;
    readonly threads: ReadonlyArray<Pick<OrchestrationThreadShell, "projectId" | "worktreePath">>;
  },
  identifier: string,
  resolvedPath: string,
): OrchestrationProjectShell | undefined => {
  const byId = snapshot.projects.find((project) => project.id === identifier);
  if (byId !== undefined) return byId;
  const target = normalizeProjectPathForComparison(resolvedPath);
  const roots = [
    ...snapshot.projects.map((project) => ({ root: project.workspaceRoot, id: project.id })),
    ...snapshot.threads.flatMap((thread) =>
      thread.worktreePath === null ? [] : [{ root: thread.worktreePath, id: thread.projectId }],
    ),
  ]
    .map(({ root, id }) => ({ root: normalizeProjectPathForComparison(root), id }))
    .filter(({ root }) => isPathWithin(root, target))
    .toSorted((a, b) => b.root.length - a.root.length);
  return snapshot.projects.find((project) => project.id === roots[0]?.id);
};

const findProject = Effect.fn("findThreadProject")(function* (
  snapshot: Parameters<typeof matchProject>[0],
  identifier: string,
) {
  const path = yield* Path.Path;
  const wanted = identifier.trim();
  return (
    matchProject(snapshot, wanted, path.resolve(wanted)) ??
    (yield* new ThreadProjectNotFoundError({ project: wanted }))
  );
});

type CheckoutBranch =
  | { readonly _tag: "Branch"; readonly name: string }
  | { readonly _tag: "Detached" }
  | { readonly _tag: "NotRepository" };

/** What the project checkout has checked out, read with git. */
const readCheckoutBranch = Effect.fn("readThreadCheckoutBranch")(function* (cwd: string) {
  const runner = yield* ProcessRunner.ProcessRunner;
  const result = yield* runner
    .run({ command: "git", args: ["symbolic-ref", "--quiet", "--short", "HEAD"], cwd })
    .pipe(Effect.option);
  const name = Option.isSome(result) ? result.value.stdout.trim() : "";
  const code = Option.isSome(result) ? result.value.code : null;
  // With --quiet, exit 1 means a detached HEAD. Other failures mean git or
  // the repository is missing.
  const checkout: CheckoutBranch =
    code === 0 && name.length > 0
      ? { _tag: "Branch", name }
      : code === 1
        ? { _tag: "Detached" }
        : { _tag: "NotRepository" };
  return checkout;
});

const decodeServerSettings = Schema.decodeUnknownEffect(fromLenientJson(ServerSettings));

/** Reads settings.json, or the defaults when it is missing or invalid. */
const readServerSettings = (settingsPath: string) =>
  FileSystem.FileSystem.pipe(
    Effect.flatMap((fs) => fs.readFileString(settingsPath)),
    Effect.flatMap(decodeServerSettings),
    Effect.orElseSucceed(() => DEFAULT_SERVER_SETTINGS),
  );

/** Reads the enabled, installed providers from the server's on-disk status cache. */
const readCachedProviders = Effect.fn("readCachedProviders")(function* (cacheDir: string) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const entries = yield* fs.readDirectory(cacheDir).pipe(Effect.orElseSucceed(() => []));
  const providers = yield* Effect.forEach(
    entries.filter((entry) => entry.endsWith(".json")),
    (entry) => readProviderStatusCache(path.join(cacheDir, entry)),
  );
  return providers.flatMap((provider) =>
    provider !== undefined && provider.enabled && provider.installed ? [provider] : [],
  );
});

/** Resolves `--model` and `--provider`, or returns `fallback` when neither is set. */
const resolveModelSelection = Effect.fn("resolveThreadModelSelection")(function* (input: {
  readonly model: string | undefined;
  readonly provider: string | undefined;
  readonly fallback: ModelSelection | null | undefined;
  readonly cacheDir: string;
}) {
  if (input.model === undefined && input.provider === undefined) {
    return input.fallback ?? (yield* new ThreadModelRequiredError());
  }
  const offers = findModelOffers(
    yield* readCachedProviders(input.cacheDir),
    input.model,
    input.provider,
  );
  const [offer, ...others] = offers;
  if (offer === undefined) {
    return yield* new ThreadModelNotFoundError({
      model: input.model ?? null,
      provider: input.provider ?? null,
    });
  }
  if (others.length > 0) {
    return yield* new ThreadModelAmbiguousError({
      model: offer.model,
      providers: offers.map((candidate) => candidate.instanceId),
    });
  }
  return offer;
});

/** Connects to the running server. The session is revoked when the scope closes. */
const connectLiveServer = Effect.fn("connectThreadCliServer")(function* (
  config: ServerConfig.ServerConfig["Service"],
) {
  const runtimeState = yield* readPersistedServerRuntimeState(config.serverRuntimeStatePath);
  if (Option.isNone(runtimeState) || !isProcessAlive(runtimeState.value.pid)) {
    return yield* new ThreadServerNotRunningError();
  }
  const environmentAuth = yield* EnvironmentAuth.EnvironmentAuth;
  const session = yield* Effect.acquireRelease(
    environmentAuth.issueSession({
      scopes: [AuthOrchestrationReadScope, AuthOrchestrationOperateScope],
      label: "t3 thread cli",
      // Bounds the leak if the process is killed before it can revoke.
      ttl: Duration.days(1),
    }),
    (issued) => environmentAuth.revokeSession(issued.sessionId).pipe(Effect.ignore({ log: true })),
  );
  const client = yield* HttpApiClient.make(EnvironmentHttpApi, {
    baseUrl: runtimeState.value.origin,
  });
  const headers = { authorization: `Bearer ${session.token}` };
  const read = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.timeout(THREAD_CLI_REQUEST_TIMEOUT),
      Effect.mapError(projectCommandErrorFromLiveServerRequest),
    );
  const orThreadNotFound =
    (threadId: ThreadId) =>
    <A, R>(effect: Effect.Effect<A, ProjectCommandError, R>) =>
      effect.pipe(
        Effect.catchIf(
          (error) =>
            error._tag === "ProjectLiveServerDeclaredResponseError" && error.code === "not_found",
          () => new ThreadNotFoundError({ threadId }),
        ),
      );

  return {
    shell: read(client.orchestration.shellSnapshot({ headers })),
    /** Reads one thread's shell: status, latest turn, and pending requests. */
    threadShell: (threadId: ThreadId) =>
      read(client.orchestration.threadShell({ headers, params: { threadId } })).pipe(
        orThreadNotFound(threadId),
      ),
    /** Reads the messages of `turns` user turns, the latest ones or those before `beforeCursor`. */
    threadDetail: (threadId: ThreadId, turns: number, beforeCursor?: string) =>
      read(
        client.orchestration.threadSnapshot({
          headers,
          params: { threadId },
          payload: { turnLimit: turns, ...(beforeCursor === undefined ? {} : { beforeCursor }) },
        }),
      ).pipe(orThreadNotFound(threadId)),
    /**
     * No timeout: a bootstrap turn start returns only after the worktree and a
     * blocking setup script are ready, which can take minutes.
     */
    dispatch: (command: ClientOrchestrationCommand) =>
      // The client types each command variant as its own request, so a union
      // payload needs the cast. `t3 project` does the same.
      client.orchestration
        .dispatch({ headers, payload: command } as Parameters<
          typeof client.orchestration.dispatch
        >[0])
        .pipe(Effect.mapError(projectCommandErrorFromLiveServerRequest)),
  };
});

type LiveServer = Effect.Success<ReturnType<typeof connectLiveServer>>;

/** Reads the thread every poll interval until `done` returns a value. */
const pollThread = <A>(
  server: LiveServer,
  threadId: ThreadId,
  done: (thread: OrchestrationThreadShell) => A | undefined,
) =>
  Effect.gen(function* () {
    for (;;) {
      const result = done(yield* server.threadShell(threadId));
      if (result !== undefined) return result;
      yield* Effect.sleep(THREAD_POLL_INTERVAL);
    }
  });

/**
 * Reads pages of the thread, newest first, until it finds `messageId`.
 * Returns that message and the messages after it, until the next user message.
 * Dispatch returns after the projection commits, so a sent message is there.
 */
const readSentTurn = (server: LiveServer, threadId: ThreadId, messageId: MessageId) =>
  Effect.gen(function* () {
    let newer: ReadonlyArray<OrchestrationMessage> = [];
    let beforeCursor: string | undefined;
    for (;;) {
      const { thread, page } = yield* server.threadDetail(threadId, 1, beforeCursor);
      const messages = [...thread.messages, ...newer];
      const index = messages.findIndex((message) => message.id === messageId);
      const sent = messages[index];
      if (sent !== undefined) {
        const after = messages.slice(index + 1);
        const nextUser = after.findIndex((message) => message.role === "user");
        return { sent, replies: nextUser === -1 ? after : after.slice(0, nextUser) };
      }
      if (page?.beforeCursor == null) {
        return yield* Effect.die(
          new Error(`Sent message ${messageId} is missing from the thread.`),
        );
      }
      newer = messages;
      beforeCursor = page.beforeCursor;
    }
  });

/**
 * Waits until the server has handled `messageId` and returns the agent's
 * final reply. Fails when the turn fails, is interrupted, or needs the user.
 */
const waitForReply = (server: LiveServer, threadId: ThreadId, messageId: MessageId) =>
  Effect.gen(function* () {
    // The server replaces the command's createdAt with its own clock, so
    // read the stamp back from the message.
    const { sent } = yield* readSentTurn(server, threadId, messageId);
    const ended = yield* pollThread(server, threadId, (candidate) => {
      const outcome = turnOutcome(candidate, sent.createdAt);
      return outcome === undefined ? undefined : { outcome, thread: candidate };
    });
    if (ended.outcome !== "completed") {
      return yield* new ThreadTurnEndedError({
        outcome: ended.outcome,
        detail: ended.thread.session?.lastError ?? null,
      });
    }
    const { replies } = yield* readSentTurn(server, threadId, messageId);
    return replies.findLast((message) => message.role === "assistant")?.text ?? "";
  });

/**
 * The `--json` result of `start` and `send`. After a wait, the thread is read
 * again: the server renames a new worktree's temporary branch after the first
 * turn, as it does for threads started in the app.
 */
const turnResult = (
  server: LiveServer,
  thread: OrchestrationThreadShell,
  reply: string | undefined,
) =>
  Effect.gen(function* () {
    const current = reply === undefined ? thread : yield* server.threadShell(thread.id);
    return {
      threadId: thread.id,
      branch: current.branch,
      worktreePath: current.worktreePath,
      ...(reply === undefined ? {} : { reply }),
    };
  });

/** Runs `run` against the live server and prints what it returns. */
const runWithLiveServer = <E, R>(
  flags: { readonly baseDir: Option.Option<string>; readonly json?: boolean },
  run: (
    server: LiveServer,
    config: ServerConfig.ServerConfig["Service"],
  ) => Effect.Effect<string, E, R>,
) =>
  Effect.gen(function* () {
    const config = yield* resolveCliAuthConfig(flags, yield* GlobalFlag.LogLevel);
    // Keep server logs out of output that scripts parse.
    const logLevel = flags.json === true ? "Error" : config.logLevel;
    return yield* Effect.gen(function* () {
      yield* Console.log(yield* run(yield* connectLiveServer(config), config));
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(
          EnvironmentAuth.runtimeLayer,
          T3ProjectFileLoader.layer,
          ProcessRunner.layer,
        ).pipe(
          Layer.provideMerge(FetchHttpClient.layer),
          Layer.provide(ServerConfig.layer(config)),
          Layer.provideMerge(Layer.succeed(References.MinimumLogLevel, logLevel)),
        ),
      ),
    );
  });

const threadIdArgument = Argument.String("thread").pipe(
  Argument.withSchema(ThreadId),
  Argument.withDescription("Thread id, from `t3 thread list`."),
);

const messageArgument = Argument.String("message").pipe(
  Argument.withDescription("Message to send, or `-` to read it from stdin."),
);

const waitFlag = Flag.Boolean("wait").pipe(
  Flag.withDescription("Wait for the turn to end and print the agent's reply."),
  Flag.withDefault(false),
);

const jsonFlag = Flag.Boolean("json").pipe(
  Flag.withDescription("Print JSON."),
  Flag.withDefault(false),
);

const threadListCommand = Command.make("list", {
  ...projectLocationFlags,
  project: Flag.String("project").pipe(
    Flag.withDescription("Only list threads in this project (id or path)."),
    Flag.optional,
  ),
  json: jsonFlag,
}).pipe(
  Command.withDescription("List threads that are not archived, most recent first."),
  Command.withHandler((flags) =>
    runWithLiveServer(flags, (server) =>
      Effect.gen(function* () {
        const snapshot = yield* server.shell;
        const projects = Option.isSome(flags.project)
          ? [yield* findProject(snapshot, flags.project.value)]
          : snapshot.projects;
        const projectTitles = new Map(projects.map((project) => [project.id, project.title]));
        const threads = snapshot.threads
          .filter((thread) => thread.archivedAt === null && projectTitles.has(thread.projectId))
          .toSorted(byLastActivity)
          .map((thread) => ({
            id: thread.id,
            title: thread.title,
            project: projectTitles.get(thread.projectId)!,
            status: threadStatus(thread),
            lastActivityAt: lastActivityAt(thread),
          }));
        if (flags.json) {
          return yield* encodeThreadList(threads);
        }
        if (threads.length === 0) {
          return "No threads.";
        }
        return threads
          .map((thread) => `${thread.id}  ${thread.status}  ${thread.project}  ${thread.title}`)
          .join("\n");
      }),
    ),
  ),
);

const threadShowCommand = Command.make("show", {
  ...projectLocationFlags,
  thread: threadIdArgument,
  turns: Flag.Int("turns").pipe(
    Flag.withSchema(Schema.Int.check(Schema.isGreaterThanOrEqualTo(1))),
    Flag.withDescription("How many of the latest turns to print."),
    Flag.withDefault(1),
  ),
  json: jsonFlag,
}).pipe(
  Command.withDescription("Print the latest messages in a thread."),
  Command.withHandler((flags) =>
    runWithLiveServer(flags, (server) =>
      Effect.gen(function* () {
        const { thread } = yield* server.threadDetail(flags.thread, flags.turns);
        const messages = thread.messages
          .filter((message) => message.role === "user" || message.role === "assistant")
          .map(({ id, role, text, createdAt }) => ({ id, role, text, createdAt }));
        const status = thread.session?.status ?? "idle";
        if (flags.json) {
          return yield* encodeThreadShow({ id: thread.id, title: thread.title, status, messages });
        }
        return [
          `${thread.title} (${status})`,
          ...messages.map((message) => `\n[${message.role}]\n${message.text}`),
        ].join("\n");
      }),
    ),
  ),
);

const threadSendCommand = Command.make("send", {
  ...projectLocationFlags,
  thread: threadIdArgument,
  message: messageArgument,
  wait: waitFlag,
  json: jsonFlag,
}).pipe(
  Command.withDescription(
    "Send a message to a thread. If the agent is working, waits for its turn to end first.",
  ),
  Command.withHandler((flags) =>
    runWithLiveServer(flags, (server) =>
      Effect.gen(function* () {
        const text = yield* readMessage(flags.message);
        let thread = yield* server.threadShell(flags.thread);
        // Sending to a busy thread steers its running turn, and some providers
        // interrupt the agent to do it. Queue behind the turn instead.
        if (isThreadBusy(thread)) {
          yield* Console.error("Waiting for the current turn to end...");
          thread = yield* pollThread(server, flags.thread, (candidate) =>
            isThreadBusy(candidate) ? undefined : candidate,
          );
        }
        if (thread.hasPendingApprovals || thread.hasPendingUserInput) {
          return yield* new ThreadTurnEndedError({ outcome: "needs-input", detail: null });
        }

        const messageId = MessageId.make(yield* threadCliUuid);
        yield* server.dispatch({
          type: "thread.turn.start",
          commandId: CommandId.make(yield* threadCliUuid),
          threadId: thread.id,
          message: { messageId, role: "user", text, attachments: [] },
          runtimeMode: thread.runtimeMode,
          interactionMode: thread.interactionMode,
          createdAt: DateTime.formatIso(yield* DateTime.now),
        });
        const reply = flags.wait ? yield* waitForReply(server, thread.id, messageId) : undefined;
        if (flags.json) {
          return yield* encodeTurnResult(yield* turnResult(server, thread, reply));
        }
        return reply ?? `Sent to ${thread.title}.`;
      }),
    ),
  ),
);

const threadStartCommand = Command.make("start", {
  ...projectLocationFlags,
  project: Argument.String("project").pipe(
    Argument.withDescription(
      "Project id or path. A path inside the project or one of its worktrees also works, so `.` works from an agent's worktree.",
    ),
  ),
  message: messageArgument,
  model: Flag.String("model").pipe(
    Flag.withDescription(
      "Model slug, like `claude-sonnet-5`. Default: the project default, else the model of the latest thread.",
    ),
    Flag.optional,
  ),
  provider: Flag.String("provider").pipe(
    Flag.withDescription(
      "Provider instance, like `codex` or `claudeAgent`. Without --model, uses its default model.",
    ),
    Flag.optional,
  ),
  worktree: Flag.Boolean("worktree").pipe(
    Flag.withDescription("Run in a new worktree. Fails if the project cannot have one."),
    Flag.withDefault(false),
  ),
  local: Flag.Boolean("local").pipe(
    Flag.withDescription("Run in the project folder."),
    Flag.withDefault(false),
  ),
  base: Flag.String("base").pipe(
    Flag.withDescription(
      "Branch a new worktree starts from. Default: the branch the project folder is on.",
    ),
    Flag.optional,
  ),
  wait: waitFlag,
  json: jsonFlag,
}).pipe(
  Command.withDescription(
    "Start a thread with a first message, like the app's composer, and print its id. Uses the project's default for a new worktree or the project folder.",
  ),
  Command.withHandler((flags) =>
    runWithLiveServer(flags, (server, config) =>
      Effect.gen(function* () {
        if (flags.worktree && flags.local) {
          return yield* new ThreadEnvModeConflictError();
        }
        const text = yield* readMessage(flags.message);
        const snapshot = yield* server.shell;
        const project = yield* findProject(snapshot, flags.project);
        const settings = yield* readServerSettings(config.settingsPath);
        const projectFile = yield* T3ProjectFileLoader.T3ProjectFileLoader.pipe(
          Effect.flatMap((loader) => loader.load(project.workspaceRoot)),
        );
        const projectSettings = resolveProjectSettings(
          settings,
          project.id,
          project,
          Option.getOrNull(projectFile),
        ).settings;
        // Like the composer: the project default, else the latest thread's model.
        const recentThreads = snapshot.threads.toSorted(byLastActivity);
        const fallback = [
          projectSettings.defaultModelSelection,
          recentThreads.find((thread) => thread.projectId === project.id)?.modelSelection,
          recentThreads[0]?.modelSelection,
        ].find(
          (selection) => selection != null && isModelSelectionProviderEnabled(settings, selection),
        );
        const modelSelection = yield* resolveModelSelection({
          model: Option.getOrUndefined(flags.model),
          provider: Option.getOrUndefined(flags.provider),
          fallback,
          cacheDir: config.providerStatusCacheDir,
        });

        // Like the composer: the project's default mode, and a worktree starts
        // from the branch the project folder is on unless --base says otherwise.
        const checkout = yield* readCheckoutBranch(project.workspaceRoot);
        if (flags.worktree && checkout._tag === "NotRepository") {
          return yield* new ThreadWorktreeUnavailableError();
        }
        const requestedMode: ThreadEnvMode = flags.worktree
          ? "worktree"
          : flags.local
            ? "local"
            : projectSettings.defaultThreadEnvMode;
        const envMode = checkout._tag === "NotRepository" ? "local" : requestedMode;
        const currentBranch = checkout._tag === "Branch" ? checkout.name : null;
        const worktreeBase =
          envMode === "worktree" ? (Option.getOrUndefined(flags.base) ?? currentBranch) : null;
        if (envMode === "worktree" && worktreeBase === null) {
          return yield* new ThreadBaseBranchRequiredError();
        }

        const threadId = ThreadId.make(yield* threadCliUuid);
        const messageId = MessageId.make(yield* threadCliUuid);
        const worktreeBranchToken = yield* threadCliUuid;
        const title = truncate(text);
        const runtimeMode = projectSettings.defaultRuntimeMode;
        const interactionMode = DEFAULT_PROVIDER_INTERACTION_MODE;
        const createdAt = DateTime.formatIso(yield* DateTime.now);
        if (worktreeBase !== null) {
          yield* Console.error(`Creating a worktree from ${worktreeBase}...`);
        }
        // One command, the same one the composer sends: the server creates the
        // thread, prepares the worktree, runs setup, then starts the turn. A
        // failure rolls the thread back.
        yield* server.dispatch({
          type: "thread.turn.start",
          commandId: CommandId.make(yield* threadCliUuid),
          threadId,
          message: { messageId, role: "user", text, attachments: [] },
          modelSelection,
          titleSeed: title,
          runtimeMode,
          interactionMode,
          bootstrap: {
            createThread: {
              projectId: project.id,
              title,
              modelSelection,
              runtimeMode,
              interactionMode,
              branch: worktreeBase ?? currentBranch,
              worktreePath: null,
              createdAt,
            },
            ...(worktreeBase === null
              ? {}
              : {
                  prepareWorktree: {
                    projectCwd: project.workspaceRoot,
                    baseBranch: worktreeBase,
                    branch: buildTemporaryWorktreeBranchName(() => worktreeBranchToken),
                    ...(projectSettings.newWorktreesStartFromOrigin
                      ? { startFromOrigin: true }
                      : {}),
                    // An explicit --worktree must not fall back to the project folder.
                    ...(flags.worktree ? { requireWorktree: true } : {}),
                  },
                  runSetupScript: true,
                }),
          },
          createdAt,
        });

        const thread = yield* server.threadShell(threadId);
        yield* Console.error(
          thread.worktreePath === null
            ? `Started thread ${threadId} in the project folder.`
            : `Started thread ${threadId} in ${thread.worktreePath} (${thread.branch ?? "no branch"}).`,
        );
        const reply = flags.wait ? yield* waitForReply(server, threadId, messageId) : undefined;
        if (flags.json) {
          return yield* encodeTurnResult(yield* turnResult(server, thread, reply));
        }
        return reply ?? threadId;
      }),
    ),
  ),
);

export const threadCommand = Command.make("thread").pipe(
  Command.withDescription("List, start, read, and message threads on the running T3 Code server."),
  Command.withSubcommands([
    threadListCommand,
    threadStartCommand,
    threadShowCommand,
    threadSendCommand,
  ]),
);
