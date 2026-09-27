import type {
  OrchestrationV2BackgroundWorkKind,
  OrchestrationV2ConversationMessage,
  OrchestrationV2Notification,
  OrchestrationV2Subagent,
  OrchestrationV2TurnItem,
  ThreadId,
} from "@t3tools/contracts";

type NotificationOutcome = OrchestrationV2Notification["outcome"];

/** One piece of background work an adapter saw finish or report, as the user should read it. */
export interface BackgroundWorkReport {
  readonly kind: OrchestrationV2BackgroundWorkKind;
  /** Subagent title, command, or monitor description. */
  readonly label?: string | undefined;
  readonly outcome: NotificationOutcome;
  readonly exitCode?: number | undefined;
  readonly childThreadId?: ThreadId | undefined;
}

const LABEL_MAX_LENGTH = 80;

function reportLabel(label: string | undefined): string | undefined {
  const firstLine = label?.trim().split("\n")[0]?.trim();
  if (firstLine === undefined || firstLine.length === 0) return undefined;
  return firstLine.length > LABEL_MAX_LENGTH
    ? `${firstLine.slice(0, LABEL_MAX_LENGTH - 1)}…`
    : firstLine;
}

const KIND_NOUN: Record<OrchestrationV2BackgroundWorkKind, readonly [string, string]> = {
  subagent: ["Subagent", "subagents"],
  command: ["Command", "commands"],
  monitor: ["Monitor", "monitors"],
  task: ["Background task", "background tasks"],
};

function outcomeVerb(kind: OrchestrationV2BackgroundWorkKind, outcome: NotificationOutcome) {
  switch (outcome) {
    case "failed":
      return "failed";
    case "cancelled":
      return "was stopped";
    case "updated":
      return kind === "monitor" ? "reported new output" : "updated";
    case "completed":
    case "unknown":
      return "finished";
  }
}

function combinedOutcome(outcomes: ReadonlyArray<NotificationOutcome>): NotificationOutcome {
  if (outcomes.includes("failed")) return "failed";
  if (outcomes.includes("cancelled")) return "cancelled";
  if (outcomes.length > 0 && outcomes.every((outcome) => outcome === "completed")) {
    return "completed";
  }
  return outcomes.includes("updated") ? "updated" : "unknown";
}

function namedReport(report: BackgroundWorkReport, capitalize: boolean): string {
  const noun = KIND_NOUN[report.kind][0];
  const label = reportLabel(report.label);
  const exit =
    report.kind === "command" && report.exitCode !== undefined ? ` (exit ${report.exitCode})` : "";
  const named = `${capitalize ? noun : noun.toLowerCase()}${label === undefined ? "" : ` "${label}"`}`;
  return `${named}${exit}`;
}

function joinNames(names: ReadonlyArray<string>): string {
  if (names.length <= 1) return names.join("");
  return `${names.slice(0, -1).join(", ")} and ${names.at(-1)}`;
}

function reportsSummary(
  reports: readonly [BackgroundWorkReport, ...ReadonlyArray<BackgroundWorkReport>],
  outcome: NotificationOutcome,
): string {
  const [first] = reports;
  if (reports.length === 1) {
    const label = reportLabel(first.label);
    const noun = KIND_NOUN[first.kind][0];
    const exit =
      first.kind === "command" && first.exitCode !== undefined ? ` (exit ${first.exitCode})` : "";
    return `${noun}${label === undefined ? "" : ` "${label}"`} ${outcomeVerb(first.kind, first.outcome)}${exit}`;
  }
  const verb =
    outcome === "failed"
      ? "failed"
      : outcome === "cancelled"
        ? "were stopped"
        : outcome === "updated"
          ? "updated"
          : "finished";
  if (reports.length > 3 || reports.every((report) => reportLabel(report.label) === undefined)) {
    const kinds = new Set(reports.map((report) => report.kind));
    const noun = kinds.size === 1 ? KIND_NOUN[first.kind][1] : "background tasks";
    return `${reports.length} ${noun} ${verb}`;
  }
  return `${joinNames(reports.map((report, index) => namedReport(report, index === 0)))} ${verb}`;
}

/**
 * A notification that says which background work a provider reported. Null
 * when nothing is known, so the caller keeps its generic notification.
 */
export function backgroundWorkNotification(
  reports: readonly [BackgroundWorkReport, ...ReadonlyArray<BackgroundWorkReport>],
  source?: OrchestrationV2Notification["source"],
): OrchestrationV2Notification;
export function backgroundWorkNotification(
  reports: ReadonlyArray<BackgroundWorkReport>,
  source?: OrchestrationV2Notification["source"],
): OrchestrationV2Notification | null;
export function backgroundWorkNotification(
  reports: ReadonlyArray<BackgroundWorkReport>,
  source: OrchestrationV2Notification["source"] = { kind: "background_task" },
): OrchestrationV2Notification | null {
  const [first, ...rest] = reports;
  if (first === undefined) return null;
  const outcome = combinedOutcome(reports.map((report) => report.outcome));
  const workKind = rest.every((report) => report.kind === first.kind) ? first.kind : undefined;
  // One subagent is the thing the row opens; a mixed or plural report opens nothing.
  const childThreadId =
    rest.length === 0 && first.kind === "subagent" ? first.childThreadId : undefined;
  return {
    source,
    outcome,
    summary: reportsSummary([first, ...rest], outcome),
    ...(workKind === undefined ? {} : { workKind }),
    ...(childThreadId === undefined ? {} : { childThreadId }),
  };
}

function delegatedTaskReport(task: OrchestrationV2Subagent | undefined): BackgroundWorkReport {
  const outcome: NotificationOutcome =
    task?.status === "failed"
      ? "failed"
      : task?.status === "cancelled" || task?.status === "interrupted"
        ? "cancelled"
        : task?.status === "completed"
          ? "completed"
          : "unknown";
  return {
    kind: "subagent",
    // Delegated tasks are titled by their prompt unless the caller named them.
    label: task?.title?.trim() || task?.prompt,
    outcome,
    ...(task?.childThreadId == null ? {} : { childThreadId: task.childThreadId }),
  };
}

/** Summarizes the delegated tasks one completion delivery reports, out of all its parent run delegated. */
function delegatedCompletionNotification(
  completion: NonNullable<OrchestrationV2ConversationMessage["delegatedCompletion"]>,
  tasks: ReadonlyArray<OrchestrationV2Subagent>,
): OrchestrationV2Notification {
  const taskIds = completion.taskIds;
  const reports = taskIds.map((taskId) =>
    delegatedTaskReport(tasks.find((task) => task.id === taskId)),
  );
  const source = { kind: "delegated_task", taskIds } as const;
  const delegatedByRun = tasks.filter(
    (task) => task.origin === "app_owned" && task.runId === completion.parentRunId,
  ).length;
  const outcome = combinedOutcome(reports.map((report) => report.outcome));
  const verb = outcome === "failed" ? "failed" : outcome === "cancelled" ? "stopped" : "finished";
  if (reports.length === 1) {
    const label = reportLabel(reports[0]?.label);
    return {
      source,
      outcome,
      summary: `Delegated task${label === undefined ? "" : ` "${label}"`} ${verb}`,
      workKind: "subagent",
      ...(reports[0]?.childThreadId === undefined
        ? {}
        : { childThreadId: reports[0].childThreadId }),
    };
  }
  const labels = reports.flatMap((report) => reportLabel(report.label) ?? []);
  const count =
    delegatedByRun > taskIds.length
      ? `${taskIds.length} of ${delegatedByRun}`
      : `${taskIds.length}`;
  return {
    source,
    outcome,
    summary: `${count} delegated tasks ${verb}${labels.length === 0 ? "" : `: ${labels.join(", ")}`}`,
    workKind: "subagent",
  };
}

/** Keep the delivery message intact while projecting its trigger as an activity. */
export function notificationTurnItem(
  item: OrchestrationV2TurnItem,
  message: Pick<OrchestrationV2ConversationMessage, "notification" | "delegatedCompletion">,
  tasks: ReadonlyArray<OrchestrationV2Subagent>,
): OrchestrationV2TurnItem {
  if (item.type !== "user_message") return item;
  const notification =
    message.delegatedCompletion === undefined
      ? message.notification
      : delegatedCompletionNotification(message.delegatedCompletion, tasks);
  if (notification === undefined) return item;
  const {
    type: _type,
    messageId: _messageId,
    inputIntent: _inputIntent,
    text: _text,
    attachments: _attachments,
    createdBy: _createdBy,
    creationSource: _creationSource,
    scheduledTaskId: _scheduledTaskId,
    ...base
  } = item;
  return { ...base, type: "notification", ...notification };
}
