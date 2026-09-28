import { SourceControlProviderRegistry } from "../sourceControl/SourceControlProviderRegistry.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  ContextTransferId,
  EventId,
  MessageId,
  type ModelSelection,
  NodeId,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  type OrchestrationV2Subagent,
  type OrchestrationV2ThreadProjection,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

import * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import { ServerConfig } from "../config.ts";
import { layer as mcpSessionRegistryTestLayer } from "../mcp/McpSessionRegistry.testkit.ts";
import { OrchestrationEngineService } from "../orchestration/Services/OrchestrationEngine.ts";
import { OrchestrationLayerLive } from "../orchestration/runtimeLayer.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { ProjectEnrichmentService } from "../project/ProjectEnrichmentService.ts";
import type { ProviderInstance } from "../provider/ProviderDriver.ts";
import { ProviderInstanceRegistry } from "../provider/Services/ProviderInstanceRegistry.ts";
import { ServerSettingsService } from "../serverSettings.ts";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import { EventSinkV2 } from "./EventSink.ts";
import { OrchestratorV2 } from "./Orchestrator.ts";
import type { ProviderAdapterV2Shape } from "./ProviderAdapter.ts";
import { OrchestrationV2EventSinkLayerLive, OrchestrationV2LayerLive } from "./runtimeLayer.ts";
import { worktreeRepairDependenciesTestLayer } from "./ProviderTurnStartService.testkit.ts";
import { makeSubagentChildThread } from "./SubagentProjection.ts";

const PlatformTestLayer = Layer.merge(
  NodeServices.layer,
  Layer.mock(SourceControlProviderRegistry)({ resolveLink: () => Effect.die("unused title link") }),
);

const ServerConfigLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-orchestration-v2-delegated-completion-",
});

const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.4",
} satisfies ModelSelection;

const VcsDriverRegistryTestLayer = VcsDriverRegistry.layer.pipe(
  Layer.provide(VcsProcess.layer),
  Layer.provide(ServerConfigLayer),
  Layer.provide(PlatformTestLayer),
);

const CheckpointStoreTestLayer = CheckpointStore.layer.pipe(
  Layer.provide(VcsDriverRegistryTestLayer),
);

const driver = ProviderDriverKind.make("codex");
const orchestrationAdapter = {
  instanceId: modelSelection.instanceId,
  driver,
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
  openSession: () => Effect.die("sessions are not used by delegated completion tests"),
} as ProviderAdapterV2Shape;
const providerInstance = {
  instanceId: modelSelection.instanceId,
  driverKind: driver,
  continuationIdentity: {
    driverKind: driver,
    continuationKey: "codex:test",
  },
  displayName: "Codex test",
  enabled: true,
  // No supportedRuntimeModes: every runtime mode runs as stored.
  snapshot: { getSnapshot: Effect.succeed({}) } as unknown as ProviderInstance["snapshot"],
  orchestrationAdapter,
  textGeneration: {} as ProviderInstance["textGeneration"],
} satisfies ProviderInstance;

const TestProviderInstanceRegistry = Layer.succeed(ProviderInstanceRegistry, {
  getInstance: (instanceId) =>
    Effect.succeed(instanceId === providerInstance.instanceId ? providerInstance : undefined),
  listInstances: Effect.succeed([providerInstance]),
  listUnavailable: Effect.succeed([]),
  streamChanges: Stream.empty,
  subscribeChanges: Effect.never,
});

// Everything but persistence, so a test can seed a database before the
// orchestrator's startup recovery reads it.
const OrchestratorOverSharedPersistenceLayer = Layer.mergeAll(
  OrchestrationLayerLive,
  OrchestrationV2LayerLive,
  OrchestrationV2EventSinkLayerLive,
).pipe(
  Layer.provide(worktreeRepairDependenciesTestLayer),
  Layer.provide(
    Layer.succeed(ProjectEnrichmentService, {
      peek: () =>
        Effect.succeed({
          repositoryIdentity: null,
          faviconPath: null,
          repositoryIdentityResolved: false,
        }),
      request: () => Effect.void,
      getAvailable: () =>
        Effect.succeed({
          repositoryIdentity: null,
          faviconPath: null,
          repositoryIdentityResolved: false,
        }),
      invalidate: () => Effect.void,
      subscribeChanges: Effect.never,
    }),
  ),
  Layer.provide(mcpSessionRegistryTestLayer),
  Layer.provide(CheckpointStoreTestLayer),
  Layer.provide(ServerConfigLayer),
  Layer.provide(ServerSettingsService.layerTest()),
  Layer.provide(TestProviderInstanceRegistry),
  Layer.provide(PlatformTestLayer),
);

const TestLayer = OrchestratorOverSharedPersistenceLayer.pipe(
  Layer.provide(SqlitePersistenceMemory),
);

const seedParentWithTerminalTask = (input: {
  readonly threadId: ThreadId;
  readonly projectId: ProjectId;
  readonly runId: RunId;
  readonly rootNodeId: NodeId;
  readonly taskId: NodeId;
  readonly deliveryState: "delivered" | "claimed" | "acknowledged" | "disposed";
  readonly completionWake?: "always" | "settled_only";
  readonly deliveryTaskIds?: ReadonlyArray<NodeId>;
  readonly now: DateTime.Utc;
}) =>
  Effect.gen(function* () {
    const applicationEngine = yield* OrchestrationEngineService;
    const orchestrator = yield* OrchestratorV2;
    const eventSink = yield* EventSinkV2;
    const providerThreadId = ProviderThreadId.make(
      `provider-thread:${String(input.threadId).replace("thread:", "")}`,
    );

    yield* applicationEngine.dispatch({
      type: "project.create",
      commandId: CommandId.make(`command:seed-project:${input.threadId}`),
      projectId: input.projectId,
      title: "Delegated completion delivery",
      workspaceRoot: `/workspace/${input.projectId}`,
      defaultModelSelection: modelSelection,
      scripts: [],
      createdAt: DateTime.formatIso(input.now),
    });

    yield* orchestrator.dispatch({
      type: "thread.create",
      createdBy: "user",
      creationSource: "web",
      commandId: CommandId.make(`command:seed-create:${input.threadId}`),
      threadId: input.threadId,
      projectId: input.projectId,
      title: "Delegated completion delivery",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
    });

    yield* eventSink.write({
      commandId: CommandId.make(`command:seed-projection:${input.threadId}`),
      events: [
        {
          id: EventId.make(`event:seed-provider-thread:${input.threadId}`),
          type: "provider-thread.updated",
          threadId: input.threadId,
          driver,
          providerInstanceId: modelSelection.instanceId,
          occurredAt: input.now,
          payload: {
            id: providerThreadId,
            driver,
            providerInstanceId: modelSelection.instanceId,
            providerSessionId: null,
            appThreadId: input.threadId,
            ownerNodeId: input.rootNodeId,
            nativeThreadRef: {
              driver,
              nativeId: `native:${input.threadId}`,
              strength: "strong",
            },
            nativeConversationHeadRef: null,
            status: "active",
            firstRunOrdinal: 1,
            lastRunOrdinal: 1,
            handoffIds: [],
            forkedFrom: null,
            createdAt: input.now,
            updatedAt: input.now,
          },
        },
        {
          id: EventId.make(`event:seed-run:${input.threadId}`),
          type: "run.updated",
          threadId: input.threadId,
          runId: input.runId,
          nodeId: input.rootNodeId,
          providerInstanceId: modelSelection.instanceId,
          occurredAt: input.now,
          payload: {
            id: input.runId,
            threadId: input.threadId,
            ordinal: 1,
            providerInstanceId: modelSelection.instanceId,
            modelSelection,
            providerThreadId,
            userMessageId: MessageId.make(`message:seed-user:${input.threadId}`),
            rootNodeId: input.rootNodeId,
            activeAttemptId: null,
            status: "running",
            requestedAt: input.now,
            startedAt: input.now,
            completedAt: null,
            checkpointId: null,
            contextHandoffId: null,
            delegatedCompletion: {
              disposition: "open",
              nextGeneration: 2,
              delivery:
                input.deliveryTaskIds === undefined
                  ? null
                  : {
                      generation: 1,
                      messageId: MessageId.make(`message:delegated-delivery:${input.threadId}`),
                      taskIds: input.deliveryTaskIds,
                    },
            },
          },
        },
        {
          id: EventId.make(`event:seed-task:${input.threadId}`),
          type: "subagent.updated",
          threadId: input.threadId,
          runId: input.runId,
          nodeId: input.taskId,
          driver,
          providerInstanceId: modelSelection.instanceId,
          occurredAt: input.now,
          payload: {
            id: input.taskId,
            threadId: input.threadId,
            runId: input.runId,
            parentNodeId: input.rootNodeId,
            origin: "app_owned",
            createdBy: "agent",
            driver,
            providerInstanceId: modelSelection.instanceId,
            providerThreadId: null,
            childThreadId: null,
            nativeTaskRef: null,
            prompt: "Inspect the delivered ownership edge.",
            title: null,
            model: null,
            completionWake: input.completionWake ?? "settled_only",
            completionDelivery: {
              state: input.deliveryState,
              observedByRunId: input.deliveryState === "acknowledged" ? input.runId : null,
            },
            status: "completed",
            result: "child finished",
            startedAt: input.now,
            completedAt: input.now,
            updatedAt: input.now,
          },
        },
      ],
    });
  });

it.layer(TestLayer)("delegated completion delivery repairs", (it) => {
  it.effect("acceptance batches pending siblings without acknowledging their results", () =>
    Effect.gen(function* () {
      const orchestrator = yield* OrchestratorV2;
      const sink = yield* EventSinkV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("mailbox-batch");
      const runId = RunId.make("mailbox-parent");
      const taskId = NodeId.make("mailbox-first");
      const messageId = MessageId.make(`message:delegated-delivery:${threadId}`);
      yield* seedParentWithTerminalTask({
        threadId,
        runId,
        projectId: ProjectId.make("mailbox-project"),
        rootNodeId: NodeId.make("mailbox-root"),
        taskId,
        deliveryState: "claimed",
        completionWake: "always",
        deliveryTaskIds: [taskId],
        now,
      });
      const projection = yield* orchestrator.getThreadProjection(threadId);
      const task = projection.subagents[0]!;
      const pendingIds = [NodeId.make("mailbox-second"), NodeId.make("mailbox-third")];
      yield* sink.write({
        events: [
          {
            id: EventId.make("mailbox-message"),
            type: "message.updated",
            threadId,
            runId,
            occurredAt: now,
            payload: {
              id: messageId,
              threadId,
              runId,
              nodeId: task.parentNodeId,
              role: "user",
              text: "Background task finished",
              attachments: [],
              streaming: false,
              createdBy: "agent",
              creationSource: "server",
              createdAt: now,
              updatedAt: now,
              delegatedCompletion: { parentRunId: runId, generation: 1, taskIds: [taskId] },
            },
          },
          ...pendingIds.map((id) => ({
            id: EventId.make(`event:${id}`),
            type: "subagent.updated" as const,
            threadId,
            runId,
            nodeId: id,
            occurredAt: now,
            payload: {
              ...task,
              id,
              completionDelivery: { state: "pending" as const, observedByRunId: null },
            },
          })),
        ],
      });
      yield* orchestrator.dispatch({
        type: "notification.delivery.accept",
        commandId: CommandId.make("accept-first"),
        threadId,
        messageId,
      });
      const accepted = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(
        accepted.subagents.find((row) => row.id === taskId)?.completionDelivery?.state,
        "delivered",
      );
      const cohort = accepted.runs.find((row) => row.id === runId)?.delegatedCompletion;
      assert.deepEqual(cohort?.delivery?.taskIds, pendingIds);
      assert.equal(cohort?.delivery?.generation, 2);
      for (const id of pendingIds) {
        assert.deepEqual(accepted.subagents.find((row) => row.id === id)?.completionDelivery, {
          state: "claimed",
          observedByRunId: null,
        });
      }
      yield* orchestrator.dispatch({
        type: "notification.delivery.accept",
        commandId: CommandId.make("repeat-old-acceptance"),
        threadId,
        messageId,
      });
      const duplicate = yield* orchestrator.getThreadProjection(threadId);
      assert.deepEqual(duplicate.runs.find((row) => row.id === runId)?.delegatedCompletion, cohort);
    }),
  );

  it.effect("builds completion text and metadata from the same live cohort", () =>
    Effect.gen(function* () {
      const orchestrator = yield* OrchestratorV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread:delegated-delivery-live-cohort");
      const projectId = ProjectId.make("project:delegated-delivery-live-cohort");
      const runId = RunId.make("run:delegated-delivery-live-cohort");
      const rootNodeId = NodeId.make("node:delegated-delivery-live-cohort-root");
      const firstTaskId = NodeId.make("node:delegated-delivery-live-cohort-first");
      const secondTaskId = NodeId.make("node:delegated-delivery-live-cohort-second");
      const messageId = MessageId.make(`message:delegated-delivery:${threadId}`);

      yield* seedParentWithTerminalTask({
        threadId,
        projectId,
        runId,
        rootNodeId,
        taskId: firstTaskId,
        deliveryState: "claimed",
        completionWake: "always",
        deliveryTaskIds: [firstTaskId, secondTaskId],
        now,
      });

      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("command:delegated-delivery-live-cohort"),
        threadId,
        messageId,
        text: `Delegated task ${firstTaskId} reached a terminal state.`,
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
        createdBy: "agent",
        creationSource: "server",
        delegatedCompletion: {
          parentRunId: runId,
          generation: 1,
          taskIds: [firstTaskId],
        },
      });

      const projection = yield* orchestrator.getThreadProjection(threadId);
      const message = projection.messages.find((candidate) => candidate.id === messageId);
      assert.deepEqual(message?.delegatedCompletion?.taskIds, [firstTaskId, secondTaskId]);
      assert.include(message?.text ?? "", String(firstTaskId));
      assert.include(message?.text ?? "", String(secondTaskId));
      assert.include(message?.text ?? "", "task_status");
    }),
  );

  it.effect("does not re-offer when wake-policy upgrades after delivered ownership settled", () =>
    Effect.gen(function* () {
      const orchestrator = yield* OrchestratorV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread:delegated-delivery-a1");
      const projectId = ProjectId.make("project:delegated-delivery-a1");
      const runId = RunId.make("run:delegated-delivery-a1");
      const rootNodeId = NodeId.make("node:delegated-delivery-a1-root");
      const taskId = NodeId.make("node:delegated-delivery-a1-task");

      yield* seedParentWithTerminalTask({
        threadId,
        projectId,
        runId,
        rootNodeId,
        taskId,
        deliveryState: "delivered",
        completionWake: "settled_only",
        now,
      });

      const upgrade = yield* orchestrator.dispatch({
        type: "delegated_task.wake-policy",
        commandId: CommandId.make("command:delegated-delivery-a1:wake-policy"),
        parentThreadId: threadId,
        taskId,
        completionWake: "always",
      });

      const projection = yield* orchestrator.getThreadProjection(threadId);
      const task = projection.subagents.find((candidate) => candidate.id === taskId);
      const parentRun = projection.runs.find((candidate) => candidate.id === runId);

      assert.equal(task?.completionWake, "always");
      assert.deepEqual(task?.completionDelivery, {
        state: "delivered",
        observedByRunId: null,
      });
      assert.deepEqual(parentRun?.delegatedCompletion, {
        disposition: "open",
        nextGeneration: 2,
        delivery: null,
      });
      assert.isFalse(
        upgrade.storedEvents.some(
          (stored) =>
            stored.event.type === "subagent.updated" &&
            stored.event.payload.id === taskId &&
            stored.event.payload.completionDelivery?.state === "claimed",
        ),
      );
      assert.isFalse(
        upgrade.storedEvents.some(
          (stored) =>
            stored.event.type === "run.updated" &&
            stored.event.payload.id === runId &&
            stored.event.payload.delegatedCompletion?.delivery !== null &&
            stored.event.payload.delegatedCompletion?.delivery !== undefined,
        ),
      );
    }),
  );

  it.effect(
    "treats repeated acknowledge and dispose with distinct command IDs as successful no-ops",
    () =>
      Effect.gen(function* () {
        const orchestrator = yield* OrchestratorV2;
        const now = yield* DateTime.now;
        const threadId = ThreadId.make("thread:delegated-delivery-a2");
        const projectId = ProjectId.make("project:delegated-delivery-a2");
        const runId = RunId.make("run:delegated-delivery-a2");
        const rootNodeId = NodeId.make("node:delegated-delivery-a2-root");
        const taskId = NodeId.make("node:delegated-delivery-a2-task");

        yield* seedParentWithTerminalTask({
          threadId,
          projectId,
          runId,
          rootNodeId,
          taskId,
          deliveryState: "delivered",
          completionWake: "always",
          now,
        });

        // Distinct command IDs mirror task_status vs t3_thread_read racing after
        // their shared read preflight saw delivered ownership.
        const firstAck = yield* orchestrator.dispatch({
          type: "delegated_task.completion-delivery.acknowledge",
          commandId: CommandId.make("command:delegated-delivery-a2:ack-task-status"),
          parentThreadId: threadId,
          taskId,
          observedByRunId: runId,
        });
        const secondAck = yield* orchestrator.dispatch({
          type: "delegated_task.completion-delivery.acknowledge",
          commandId: CommandId.make("command:delegated-delivery-a2:ack-thread-read"),
          parentThreadId: threadId,
          taskId,
          observedByRunId: runId,
        });

        const firstAckTask = firstAck.storedEvents.find(
          (stored) =>
            stored.event.type === "subagent.updated" && stored.event.payload.id === taskId,
        );
        const secondAckTask = secondAck.storedEvents.find(
          (stored) =>
            stored.event.type === "subagent.updated" && stored.event.payload.id === taskId,
        );
        assert.isDefined(firstAckTask);
        assert.isDefined(secondAckTask);
        if (
          firstAckTask?.event.type !== "subagent.updated" ||
          secondAckTask?.event.type !== "subagent.updated"
        ) {
          return yield* Effect.die(new Error("Acknowledge events missing."));
        }
        assert.equal(firstAckTask.event.payload.completionDelivery?.state, "acknowledged");
        assert.equal(secondAckTask.event.payload.completionDelivery?.state, "acknowledged");
        // Idempotent replay keeps the first observation's ownership and timestamp.
        assert.deepEqual(
          secondAckTask.event.payload.completionDelivery,
          firstAckTask.event.payload.completionDelivery,
        );
        assert.deepEqual(
          secondAckTask.event.payload.updatedAt,
          firstAckTask.event.payload.updatedAt,
        );
        assert.equal(secondAck.storedEvents.length, 1);

        const afterAck = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(
          afterAck.subagents.find((candidate) => candidate.id === taskId)?.completionDelivery,
          {
            state: "acknowledged",
            observedByRunId: runId,
          },
        );

        const firstDispose = yield* orchestrator.dispatch({
          type: "delegated_task.completion-delivery.dispose",
          commandId: CommandId.make("command:delegated-delivery-a2:dispose-task-status"),
          parentThreadId: threadId,
          taskId,
        });
        const secondDispose = yield* orchestrator.dispatch({
          type: "delegated_task.completion-delivery.dispose",
          commandId: CommandId.make("command:delegated-delivery-a2:dispose-thread-read"),
          parentThreadId: threadId,
          taskId,
        });

        const firstDisposeTask = firstDispose.storedEvents.find(
          (stored) =>
            stored.event.type === "subagent.updated" && stored.event.payload.id === taskId,
        );
        const secondDisposeTask = secondDispose.storedEvents.find(
          (stored) =>
            stored.event.type === "subagent.updated" && stored.event.payload.id === taskId,
        );
        assert.isDefined(firstDisposeTask);
        assert.isDefined(secondDisposeTask);
        if (
          firstDisposeTask?.event.type !== "subagent.updated" ||
          secondDisposeTask?.event.type !== "subagent.updated"
        ) {
          return yield* Effect.die(new Error("Dispose events missing."));
        }
        assert.equal(firstDisposeTask.event.payload.completionDelivery?.state, "disposed");
        assert.equal(secondDisposeTask.event.payload.completionDelivery?.state, "disposed");
        assert.deepEqual(
          secondDisposeTask.event.payload.completionDelivery,
          firstDisposeTask.event.payload.completionDelivery,
        );
        assert.equal(secondDispose.storedEvents.length, 1);

        const afterDispose = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(
          afterDispose.subagents.find((candidate) => candidate.id === taskId)?.completionDelivery,
          {
            state: "disposed",
            observedByRunId: null,
          },
        );

        const acknowledgeAfterDispose = yield* orchestrator.dispatch({
          type: "delegated_task.completion-delivery.acknowledge",
          commandId: CommandId.make("command:delegated-delivery-a2:ack-after-dispose"),
          parentThreadId: threadId,
          taskId,
          observedByRunId: runId,
        });
        const acknowledgedTask = acknowledgeAfterDispose.storedEvents.find(
          (stored) => stored.event.type === "subagent.updated",
        );
        if (acknowledgedTask?.event.type !== "subagent.updated") {
          return yield* Effect.die(new Error("Acknowledge-after-dispose event missing."));
        }
        assert.deepEqual(acknowledgedTask.event.payload.completionDelivery, {
          state: "disposed",
          observedByRunId: null,
        });
        assert.equal(acknowledgeAfterDispose.storedEvents.length, 1);

        const afterStaleAcknowledge = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(
          afterStaleAcknowledge.subagents.find((candidate) => candidate.id === taskId)
            ?.completionDelivery,
          {
            state: "disposed",
            observedByRunId: null,
          },
        );
      }),
  );
});

/**
 * A parent whose delegated child already finished run 1: the task row is
 * terminal, its result transfer exists, and the delivery is in the given
 * state. The parent run is settled, so a new result reserves a parent wake.
 */
const seedFinishedDelegatedTask = (input: {
  readonly name: string;
  readonly delivery: OrchestrationV2Subagent["completionDelivery"];
  readonly cohortDelivery?: "claims-task";
  readonly now: DateTime.Utc;
}) =>
  Effect.gen(function* () {
    const eventSink = yield* EventSinkV2;
    const { name, now } = input;
    const parentThreadId = ThreadId.make(`thread:reopen-${name}-parent`);
    const childThreadId = ThreadId.make(`thread:reopen-${name}-child`);
    const parentRunId = RunId.make(`run:reopen-${name}-parent`);
    const parentRootNodeId = NodeId.make(`node:reopen-${name}-parent-root`);
    const taskId = NodeId.make(`node:reopen-${name}-task`);
    const parentProviderThreadId = ProviderThreadId.make(`provider-thread:reopen-${name}-parent`);
    const childProviderThreadId = ProviderThreadId.make(`provider-thread:reopen-${name}-child`);
    const deliveryMessageId = MessageId.make(`message:reopen-${name}-delivery`);
    const parentThread: OrchestrationV2AppThread = {
      createdBy: "user",
      creationSource: "web",
      id: parentThreadId,
      projectId: ProjectId.make(`project:reopen-${name}`),
      title: "Delegating parent",
      providerInstanceId: modelSelection.instanceId,
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: parentProviderThreadId,
      lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: parentThreadId },
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    };
    const childThread = makeSubagentChildThread({
      parentThread,
      childThreadId,
      parentNodeId: taskId,
      activeProviderThreadId: childProviderThreadId,
      providerInstanceId: modelSelection.instanceId,
      modelSelection,
      title: "Delegated child",
      now,
      createdBy: "agent",
      creationSource: "mcp",
    });
    const providerThread = (id: ProviderThreadId, threadId: ThreadId) => ({
      id,
      driver,
      providerInstanceId: modelSelection.instanceId,
      providerSessionId: null,
      appThreadId: threadId,
      ownerNodeId: null,
      nativeThreadRef: { driver, nativeId: `native:${id}`, strength: "strong" as const },
      nativeConversationHeadRef: null,
      status: "idle" as const,
      firstRunOrdinal: 1,
      lastRunOrdinal: 1,
      handoffIds: [],
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
    });
    const task: OrchestrationV2Subagent = {
      id: taskId,
      threadId: parentThreadId,
      runId: parentRunId,
      parentNodeId: parentRootNodeId,
      origin: "app_owned",
      createdBy: "agent",
      driver,
      providerInstanceId: modelSelection.instanceId,
      providerThreadId: childProviderThreadId,
      childThreadId,
      nativeTaskRef: null,
      prompt: "Review the change.",
      title: null,
      model: null,
      completionWake: "always",
      ...(input.delivery === undefined ? {} : { completionDelivery: input.delivery }),
      status: "completed",
      result: "first result",
      startedAt: now,
      completedAt: now,
      updatedAt: now,
    };
    const childRun1 = childRunPayload({ childThreadId, childProviderThreadId, ordinal: 1, now });
    yield* eventSink.write({
      commandId: CommandId.make(`command:reopen-${name}:seed`),
      events: [
        {
          id: nextEventId(),
          type: "thread.created",
          threadId: parentThreadId,
          occurredAt: now,
          payload: parentThread,
        },
        {
          id: nextEventId(),
          type: "provider-thread.updated",
          threadId: parentThreadId,
          driver,
          providerInstanceId: modelSelection.instanceId,
          occurredAt: now,
          payload: providerThread(parentProviderThreadId, parentThreadId),
        },
        {
          id: nextEventId(),
          type: "run.updated",
          threadId: parentThreadId,
          runId: parentRunId,
          nodeId: parentRootNodeId,
          providerInstanceId: modelSelection.instanceId,
          occurredAt: now,
          payload: {
            id: parentRunId,
            threadId: parentThreadId,
            ordinal: 1,
            providerInstanceId: modelSelection.instanceId,
            modelSelection,
            providerThreadId: parentProviderThreadId,
            userMessageId: MessageId.make(`message:reopen-${name}-parent-user`),
            rootNodeId: parentRootNodeId,
            activeAttemptId: null,
            status: "completed",
            requestedAt: now,
            startedAt: now,
            completedAt: now,
            checkpointId: null,
            contextHandoffId: null,
            delegatedCompletion: {
              disposition: "open",
              nextGeneration: 2,
              delivery:
                input.cohortDelivery === "claims-task"
                  ? { generation: 1, messageId: deliveryMessageId, taskIds: [taskId] }
                  : null,
            },
          },
        },
        {
          id: nextEventId(),
          type: "subagent.updated",
          threadId: parentThreadId,
          runId: parentRunId,
          nodeId: taskId,
          driver,
          providerInstanceId: modelSelection.instanceId,
          occurredAt: now,
          payload: task,
        },
        {
          id: nextEventId(),
          type: "thread.created",
          threadId: childThreadId,
          occurredAt: now,
          payload: childThread,
        },
        {
          id: nextEventId(),
          type: "provider-thread.updated",
          threadId: childThreadId,
          driver,
          providerInstanceId: modelSelection.instanceId,
          occurredAt: now,
          payload: providerThread(childProviderThreadId, childThreadId),
        },
        ...childRunEvents({ run: childRun1, status: "completed", result: "first result", now }),
        {
          id: nextEventId(),
          type: "context-transfer.created",
          threadId: parentThreadId,
          runId: parentRunId,
          providerInstanceId: modelSelection.instanceId,
          occurredAt: now,
          payload: {
            id: ContextTransferId.make(`context-transfer:reopen-${name}-run-1`),
            type: "subagent_result",
            sourceThreadId: childThreadId,
            targetThreadId: parentThreadId,
            sourcePoint: { threadId: childThreadId, runId: childRun1.id },
            basePoint: null,
            sourceProviderInstanceId: modelSelection.instanceId,
            targetProviderInstanceId: modelSelection.instanceId,
            targetRunId: parentRunId,
            status: "consumed",
            resolution: null,
            createdBy: "system",
            error: null,
            createdAt: now,
            updatedAt: now,
            consumedAt: now,
          },
        },
      ],
    });
    return { parentThreadId, childThreadId, childProviderThreadId, parentRunId, taskId };
  });

let eventOrdinal = 0;
const nextEventId = () => EventId.make(`event:delegated-reopen:${(eventOrdinal += 1)}`);

const childRunPayload = (input: {
  readonly childThreadId: ThreadId;
  readonly childProviderThreadId: ProviderThreadId;
  readonly ordinal: number;
  readonly now: DateTime.Utc;
}): OrchestrationV2Run => ({
  id: RunId.make(`run:${input.childThreadId}:${input.ordinal}`),
  threadId: input.childThreadId,
  ordinal: input.ordinal,
  providerInstanceId: modelSelection.instanceId,
  modelSelection,
  providerThreadId: input.childProviderThreadId,
  userMessageId: MessageId.make(`message:${input.childThreadId}:user-${input.ordinal}`),
  rootNodeId: null,
  activeAttemptId: null,
  status: "queued",
  requestedAt: input.now,
  startedAt: null,
  completedAt: null,
  checkpointId: null,
  contextHandoffId: null,
});

/** The child run moving to `status`, with its assistant reply when it has one. */
const childRunEvents = (input: {
  readonly run: OrchestrationV2Run;
  readonly status: OrchestrationV2Run["status"];
  readonly started?: boolean;
  readonly result?: string;
  readonly now: DateTime.Utc;
}): Array<OrchestrationV2DomainEvent> => {
  const started = input.started ?? input.status !== "queued";
  return [
    ...(input.result === undefined
      ? []
      : [
          {
            id: nextEventId(),
            type: "message.updated" as const,
            threadId: input.run.threadId,
            runId: input.run.id,
            occurredAt: input.now,
            payload: {
              id: MessageId.make(`message:${input.run.id}:assistant`),
              threadId: input.run.threadId,
              runId: input.run.id,
              nodeId: null,
              role: "assistant" as const,
              text: input.result,
              attachments: [],
              streaming: false,
              createdBy: "agent" as const,
              creationSource: "provider" as const,
              createdAt: input.now,
              updatedAt: input.now,
            },
          },
        ]),
    {
      id: nextEventId(),
      type: "run.updated",
      threadId: input.run.threadId,
      runId: input.run.id,
      providerInstanceId: modelSelection.instanceId,
      occurredAt: input.now,
      payload: {
        ...input.run,
        status: input.status,
        startedAt: started ? input.now : null,
        completedAt: ["queued", "starting", "running"].includes(input.status) ? null : input.now,
      },
    },
  ];
};

/** Writes child run events and waits for the parent's task row to reach `status`. */
const advanceChild = (input: {
  readonly parentThreadId: ThreadId;
  readonly taskId: NodeId;
  readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
  readonly status: OrchestrationV2Subagent["status"];
}) =>
  Effect.gen(function* () {
    const eventSink = yield* EventSinkV2;
    const afterSequence = yield* eventSink.latestSequence();
    yield* eventSink.write({ events: input.events });
    const updated = yield* eventSink
      .stream({ threadId: input.parentThreadId, afterSequence, eventType: "subagent.updated" })
      .pipe(
        Stream.filter(
          (stored) =>
            stored.event.type === "subagent.updated" &&
            stored.event.payload.id === input.taskId &&
            stored.event.payload.status === input.status,
        ),
        Stream.runHead,
      );
    if (Option.isNone(updated)) {
      return yield* Effect.die(new Error(`Task ${input.taskId} never became ${input.status}.`));
    }
    return yield* (yield* OrchestratorV2).getThreadProjection(input.parentThreadId);
  });

const resultTransferRunIds = (
  projection: Pick<OrchestrationV2ThreadProjection, "contextTransfers">,
) =>
  projection.contextTransfers
    .filter((transfer) => transfer.type === "subagent_result")
    .map((transfer) => transfer.sourcePoint.runId);

it.layer(TestLayer)("delegated tasks answering follow-ups", (it) => {
  it.effect("reopens while the child works again and delivers the follow-up result once", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const seeded = yield* seedFinishedDelegatedTask({
        name: "acknowledged",
        delivery: { state: "acknowledged", observedByRunId: null },
        now,
      });
      const run2 = childRunPayload({
        childThreadId: seeded.childThreadId,
        childProviderThreadId: seeded.childProviderThreadId,
        ordinal: 2,
        now,
      });

      const reopened = yield* advanceChild({
        ...seeded,
        events: childRunEvents({ run: run2, status: "running", now }),
        status: "running",
      });
      const reopenedTask = reopened.subagents.find((task) => task.id === seeded.taskId);
      assert.equal(reopenedTask?.completedAt, null);
      // The earlier result's settled delivery is untouched until a new result exists.
      assert.deepEqual(reopenedTask?.completionDelivery, {
        state: "acknowledged",
        observedByRunId: null,
      });

      const finished = yield* advanceChild({
        ...seeded,
        events: childRunEvents({ run: run2, status: "completed", result: "second result", now }),
        status: "completed",
      });
      const finishedTask = finished.subagents.find((task) => task.id === seeded.taskId);
      assert.equal(finishedTask?.result, "second result");
      assert.notEqual(finishedTask?.completedAt, null);
      assert.deepEqual(finishedTask?.completionDelivery, {
        state: "claimed",
        observedByRunId: null,
      });
      assert.deepEqual(resultTransferRunIds(finished), [
        RunId.make(`run:${seeded.childThreadId}:1`),
        run2.id,
      ]);
      const cohort = finished.runs.find(
        (run) => run.id === seeded.parentRunId,
      )?.delegatedCompletion;
      assert.equal(cohort?.delivery?.generation, 2);
      assert.deepEqual(cohort?.delivery?.taskIds, [seeded.taskId]);
      assert.equal(cohort?.nextGeneration, 3);
    }),
  );

  it.effect("closes again without re-delivering when a queued follow-up never starts", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const seeded = yield* seedFinishedDelegatedTask({
        name: "cancelled-followup",
        delivery: { state: "acknowledged", observedByRunId: null },
        now,
      });
      const run2 = childRunPayload({
        childThreadId: seeded.childThreadId,
        childProviderThreadId: seeded.childProviderThreadId,
        ordinal: 2,
        now,
      });
      yield* advanceChild({
        ...seeded,
        events: childRunEvents({ run: run2, status: "queued", now }),
        status: "running",
      });
      const closed = yield* advanceChild({
        ...seeded,
        events: childRunEvents({ run: run2, status: "cancelled", started: false, now }),
        status: "completed",
      });
      const task = closed.subagents.find((candidate) => candidate.id === seeded.taskId);
      assert.equal(task?.result, "first result");
      assert.deepEqual(task?.completionDelivery, { state: "acknowledged", observedByRunId: null });
      assert.deepEqual(resultTransferRunIds(closed), [RunId.make(`run:${seeded.childThreadId}:1`)]);
      assert.deepEqual(
        closed.runs.find((run) => run.id === seeded.parentRunId)?.delegatedCompletion,
        { disposition: "open", nextGeneration: 2, delivery: null },
      );
    }),
  );

  it.effect("folds a follow-up result into a delivery that has not reached the parent", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      for (const state of ["pending", "claimed"] as const) {
        const seeded = yield* seedFinishedDelegatedTask({
          name: `unsettled-${state}`,
          delivery: { state, observedByRunId: null },
          ...(state === "claimed" ? { cohortDelivery: "claims-task" as const } : {}),
          now,
        });
        const run2 = childRunPayload({
          childThreadId: seeded.childThreadId,
          childProviderThreadId: seeded.childProviderThreadId,
          ordinal: 2,
          now,
        });
        const reopened = yield* advanceChild({
          ...seeded,
          events: childRunEvents({ run: run2, status: "running", now }),
          status: "running",
        });
        assert.deepEqual(
          reopened.subagents.find((task) => task.id === seeded.taskId)?.completionDelivery,
          { state, observedByRunId: null },
        );
        const finished = yield* advanceChild({
          ...seeded,
          events: childRunEvents({ run: run2, status: "completed", result: "second result", now }),
          status: "completed",
        });
        const task = finished.subagents.find((candidate) => candidate.id === seeded.taskId);
        assert.equal(task?.result, "second result");
        assert.deepEqual(task?.completionDelivery, { state: "claimed", observedByRunId: null });
        // One delivery carries both results: a pending task gets the first
        // reservation, and a claimed one stays in the reservation it holds.
        const cohort = finished.runs.find(
          (run) => run.id === seeded.parentRunId,
        )?.delegatedCompletion;
        assert.deepEqual(cohort?.delivery?.taskIds, [seeded.taskId]);
        assert.equal(cohort?.delivery?.generation, state === "pending" ? 2 : 1);
        assert.equal(cohort?.nextGeneration, state === "pending" ? 3 : 2);
      }
    }),
  );
});

it.effect("startup recovery reopens a terminal task whose child is already working again", () =>
  Effect.gen(function* () {
    const now = yield* DateTime.now;
    const seeded = yield* seedFinishedDelegatedTask({
      name: "startup",
      delivery: { state: "acknowledged", observedByRunId: null },
      now,
    });
    const run2 = childRunPayload({
      childThreadId: seeded.childThreadId,
      childProviderThreadId: seeded.childProviderThreadId,
      ordinal: 2,
      now,
    });
    const eventSink = yield* EventSinkV2;
    yield* eventSink.write({ events: childRunEvents({ run: run2, status: "running", now }) });

    // Recovery runs while the orchestrator layer is built over this database.
    const projection = yield* Effect.gen(function* () {
      const orchestrator = yield* OrchestratorV2;
      return yield* orchestrator.getThreadProjection(seeded.parentThreadId);
    }).pipe(Effect.provide(OrchestratorOverSharedPersistenceLayer));
    const task = projection.subagents.find((candidate) => candidate.id === seeded.taskId);
    assert.equal(task?.status, "running");
    assert.equal(task?.completedAt, null);
  }).pipe(
    Effect.provide(
      OrchestrationV2EventSinkLayerLive.pipe(Layer.provideMerge(SqlitePersistenceMemory)),
    ),
  ),
);
