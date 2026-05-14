import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import type { IncomingMessage, ServerResponse } from "node:http";
import path from "node:path";
import {
  listAgentEntries,
  listAgentIds,
  resolveAgentDir,
  resolveAgentWorkspaceDir,
  resolveDefaultAgentId,
} from "../agents/agent-scope.js";
import { bumpSkillsSnapshotVersion } from "../agents/skills/refresh-state.js";
import {
  DEFAULT_AGENTS_FILENAME,
  DEFAULT_BOOTSTRAP_FILENAME,
  DEFAULT_IDENTITY_FILENAME,
  DEFAULT_MEMORY_ALT_FILENAME,
  DEFAULT_MEMORY_FILENAME,
  ensureAgentWorkspace,
} from "../agents/workspace.js";
import { createDefaultDeps } from "../cli/deps.js";
import { agentCommandFromIngress } from "../commands/agent.js";
import { loadConfig, writeConfigFile } from "../config/config.js";
import { onAgentEvent, type AgentEventPayload } from "../infra/agent-events.js";
import type { ExecApprovalDecision } from "../infra/exec-approvals.js";
import { isPathInside } from "../infra/path-guards.js";
import { logInfo, logWarn } from "../logger.js";
import type { PromptImageOrderEntry } from "../media/prompt-image-order.js";
import { normalizeAgentId } from "../routing/session-key.js";
import { defaultRuntime } from "../runtime.js";
import { safeJsonStringify } from "../utils/safe-json.js";
import { resolveAssistantStreamDeltaText } from "./agent-event-assistant-text.js";
import {
  parseMessageWithAttachments,
  type ChatAttachment,
  type ChatImageContent,
} from "./chat-attachments.js";
import {
  loadControlPlaneRuntimeState,
  mergeControlPlaneRuntimeState,
  saveControlPlaneRuntimeState,
} from "./control-plane-runtime.js";
import type {
  ControlPlaneConversationView,
  ControlPlaneRuntimeAgent,
  ControlPlaneRuntimeRole,
} from "./control-plane-runtime.js";
import {
  installSkillPackageFromInlineArchive,
  installSkillPackageFromRegistryDownload,
} from "./control-plane-skill-install.js";
import {
  recommendSkillsFromControlPlane,
  type ControlPlaneSkillSearchResult,
} from "./control-plane-skill-registry.js";
import {
  getGlobalExecApprovalBroadcast,
  getGlobalExecApprovalForwarder,
  getGlobalExecApprovalManager,
} from "./exec-approval-context.js";
import type { ExecApprovalRecord } from "./exec-approval-manager.js";
import { setSseHeaders, writeDone } from "./http-common.js";
import { resolveSessionStoreKey } from "./session-utils.js";

// AGENT_BOT_COMPAT: HTTP bridge used by agent-bot-task-a control-plane.

type JsonObject = Record<string, unknown>;
type PortalSessionMode = "chat" | "training";
type PortalCoreWriteMode = "candidate-core" | "forbidden";
type PortalMemoryWriteMode = "candidate-core" | "user-memory";
type PortalCandidateRiskLevel = "low" | "medium" | "high";

type PortalCandidateChange = {
  kind: string;
  title: string;
  summary: string;
  currentValue: string | null;
  proposedValue: string | null;
  diffText: string | null;
  riskLevel: PortalCandidateRiskLevel;
  metadata: JsonObject;
};

type PortalApprovalSummary = {
  id: string;
  kind: "exec";
  command: string;
  host?: string;
  cwd?: string;
  expiresAt?: string;
};

type PortalWritePolicy = {
  core: PortalCoreWriteMode;
  memory: PortalMemoryWriteMode;
};

/** Stable wire shape for agent-bot control-plane → RuntimeEventRecord ingestion */
type PortalRuntimeEventWire = {
  eventType: string;
  level: "debug" | "info" | "warn" | "error";
  message: string;
  payload?: JsonObject;
  createdAt: string;
};

function buildPortalRuntimeEvent(params: {
  eventType: string;
  level: PortalRuntimeEventWire["level"];
  message: string;
  payload?: JsonObject;
  createdAt?: string;
}): PortalRuntimeEventWire {
  return {
    eventType: params.eventType,
    level: params.level,
    message: params.message,
    ...(params.payload ? { payload: params.payload } : {}),
    createdAt: params.createdAt ?? new Date().toISOString(),
  };
}

function isSseRequest(req: IncomingMessage): boolean {
  return (req.headers.accept ?? "").toLowerCase().includes("text/event-stream");
}

function writePortalStreamEvent(res: ServerResponse, type: string, data: unknown): void {
  const serialized =
    safeJsonStringify({ type, data }) ?? '{"type":"serialization.error","data":"[Unserializable]"}';
  res.write(`data: ${serialized}\n\n`);
}

function asJsonObject(value: unknown): JsonObject | undefined {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as JsonObject)
    : undefined;
}

function readFiniteNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function extractToolResultDetails(value: unknown): JsonObject | undefined {
  const record = asJsonObject(value);
  if (!record) {
    return undefined;
  }
  return asJsonObject(record.details) ?? record;
}

function buildPortalApprovalSummaryFromRecord(record: ExecApprovalRecord): PortalApprovalSummary {
  return {
    id: record.id,
    kind: "exec",
    command: record.request.command,
    host: record.request.host ?? undefined,
    cwd: record.request.cwd ?? undefined,
    expiresAt: new Date(record.expiresAtMs).toISOString(),
  };
}

function mergePortalApprovalSummary(
  current: PortalApprovalSummary | undefined,
  next: PortalApprovalSummary | undefined,
): PortalApprovalSummary | undefined {
  if (!current) {
    return next;
  }
  if (!next) {
    return current;
  }
  if (current.id !== next.id) {
    return next;
  }
  return {
    id: current.id,
    kind: next.kind ?? current.kind,
    command: next.command || current.command,
    host: next.host ?? current.host,
    cwd: next.cwd ?? current.cwd,
    expiresAt: next.expiresAt ?? current.expiresAt,
  };
}

function buildPortalApprovalSummaryFromAgentEvent(
  evt: AgentEventPayload,
): PortalApprovalSummary | undefined {
  if (evt.stream !== "tool") {
    return undefined;
  }
  const phase = typeof evt.data?.phase === "string" ? evt.data.phase : "";
  if (phase !== "result") {
    return undefined;
  }
  const toolResult = extractToolResultDetails(evt.data?.result);
  if (!toolResult || readOptionalString(toolResult, "status") !== "approval-pending") {
    return undefined;
  }
  const approvalId = readOptionalString(toolResult, "approvalId");
  const command = readOptionalString(toolResult, "command");
  if (!approvalId || !command) {
    return undefined;
  }
  const expiresAtMs = readFiniteNumber(toolResult.expiresAtMs);
  return {
    id: approvalId,
    kind: "exec",
    command,
    host: readOptionalString(toolResult, "host"),
    cwd: readOptionalString(toolResult, "cwd"),
    expiresAt: typeof expiresAtMs === "number" ? new Date(expiresAtMs).toISOString() : undefined,
  };
}

function buildPortalApprovalRequiredEvent(params: {
  runId: string;
  approval: PortalApprovalSummary;
  createdAt?: string;
}): PortalRuntimeEventWire {
  return buildPortalRuntimeEvent({
    eventType: "approval.required",
    level: "warn",
    message: "Exec approval required before continuing",
    payload: {
      runId: params.runId,
      approvalId: params.approval.id,
      kind: params.approval.kind,
      command: params.approval.command,
      host: params.approval.host ?? null,
      cwd: params.approval.cwd ?? null,
      expiresAt: params.approval.expiresAt ?? null,
    },
    createdAt: params.createdAt,
  });
}

function buildPortalRuntimeEventFromAgentEvent(
  evt: AgentEventPayload,
): PortalRuntimeEventWire | null {
  const createdAt = new Date(evt.ts).toISOString();
  if (evt.stream === "tool") {
    const phase = typeof evt.data?.phase === "string" ? evt.data.phase : "";
    const toolName = typeof evt.data?.name === "string" ? evt.data.name : "tool";
    const toolCallId = typeof evt.data?.toolCallId === "string" ? evt.data.toolCallId : undefined;
    const toolArgs = asJsonObject(evt.data?.args);
    const toolResult = extractToolResultDetails(evt.data?.result);
    const isSkillSearchTool = toolName === "skill_registry_search";
    const isSkillInstallTool = toolName === "skill_registry_install";
    const query =
      (toolArgs ? readOptionalString(toolArgs, "query", "q", "keyword") : undefined) ??
      (toolResult ? readOptionalString(toolResult, "query", "q", "keyword") : undefined);
    const skillKey =
      (toolArgs ? readOptionalString(toolArgs, "skillKey", "skill_key") : undefined) ??
      (toolResult ? readOptionalString(toolResult, "skillKey", "skill_key") : undefined);
    const version =
      (toolArgs ? readOptionalString(toolArgs, "version") : undefined) ??
      (toolResult ? readOptionalString(toolResult, "version") : undefined);
    const resultCount =
      readFiniteNumber(toolResult?.count) ?? readFiniteNumber(toolResult?.resultCount);
    const errorMessage =
      (toolResult ? readOptionalString(toolResult, "error", "message", "reason") : undefined) ??
      (typeof evt.data?.error === "string" && evt.data.error ? evt.data.error : undefined);
    const commonPayload = {
      runId: evt.runId,
      stream: evt.stream,
      phase,
      name: toolName,
      toolCallId: toolCallId ?? null,
      seq: evt.seq,
      ts: evt.ts,
    };
    if (isSkillSearchTool || isSkillInstallTool) {
      if (phase === "start") {
        return buildPortalRuntimeEvent({
          eventType: isSkillSearchTool
            ? "training.skill.search.started"
            : "training.skill.install.started",
          level: "info",
          message: isSkillSearchTool ? "开始搜索技能" : "开始安装技能",
          payload: {
            ...commonPayload,
            ...(query ? { query } : {}),
            ...(skillKey ? { skillKey, skillName: skillKey } : {}),
            ...(version ? { version } : {}),
          },
          createdAt,
        });
      }
      if (phase === "result") {
        const isError = Boolean(evt.data?.isError);
        return buildPortalRuntimeEvent({
          eventType: isSkillSearchTool
            ? isError
              ? "training.skill.search.failed"
              : "training.skill.search.completed"
            : isError
              ? "training.skill.install.failed"
              : "training.skill.install.completed",
          level: isError ? "error" : "info",
          message: isSkillSearchTool
            ? isError
              ? "技能搜索失败"
              : "技能搜索完成"
            : isError
              ? "技能安装失败"
              : "技能安装完成",
          payload: {
            ...commonPayload,
            isError,
            ...(query ? { query } : {}),
            ...(skillKey ? { skillKey, skillName: skillKey } : {}),
            ...(version ? { version } : {}),
            ...(resultCount !== undefined ? { resultCount } : {}),
            ...(toolResult?.installedPath &&
            typeof toolResult.installedPath === "string" &&
            toolResult.installedPath
              ? { installedPath: toolResult.installedPath }
              : {}),
            ...(errorMessage ? { error: errorMessage } : {}),
            ...(Array.isArray(toolResult?.items) ? { items: toolResult.items.slice(0, 5) } : {}),
          },
          createdAt,
        });
      }
    }
    if (phase === "start") {
      return buildPortalRuntimeEvent({
        eventType: "tool.started",
        level: "info",
        message: `工具 ${toolName} 开始执行`,
        payload: {
          runId: evt.runId,
          stream: evt.stream,
          phase,
          name: toolName,
          toolCallId: toolCallId ?? null,
          seq: evt.seq,
          ts: evt.ts,
        },
        createdAt,
      });
    }
    if (phase === "result") {
      const isError = Boolean(evt.data?.isError);
      return buildPortalRuntimeEvent({
        eventType: isError ? "tool.failed" : "tool.completed",
        level: isError ? "error" : "info",
        message: isError ? `工具 ${toolName} 执行失败` : `工具 ${toolName} 执行完成`,
        payload: {
          runId: evt.runId,
          stream: evt.stream,
          phase,
          name: toolName,
          toolCallId: toolCallId ?? null,
          isError,
          meta:
            evt.data?.meta && typeof evt.data.meta === "object" && !Array.isArray(evt.data.meta)
              ? evt.data.meta
              : undefined,
          result: toolResult,
          seq: evt.seq,
          ts: evt.ts,
        },
        createdAt,
      });
    }
    return null;
  }
  if (evt.stream === "lifecycle") {
    const phase = typeof evt.data?.phase === "string" ? evt.data.phase : "";
    if (!phase) {
      return null;
    }
    const level = phase === "error" ? "error" : "info";
    return buildPortalRuntimeEvent({
      eventType: `lifecycle.${phase}`,
      level,
      message:
        phase === "start"
          ? "Agent 运行已开始"
          : phase === "end"
            ? "Agent 运行已结束"
            : typeof evt.data?.error === "string" && evt.data.error
              ? evt.data.error
              : "Agent 运行失败",
      payload: {
        runId: evt.runId,
        stream: evt.stream,
        phase,
        error: typeof evt.data?.error === "string" ? evt.data.error : null,
        stopReason: typeof evt.data?.stopReason === "string" ? evt.data.stopReason : null,
        seq: evt.seq,
        ts: evt.ts,
      },
      createdAt,
    });
  }
  if (evt.stream === "error") {
    return buildPortalRuntimeEvent({
      eventType: "stream.error",
      level: "error",
      message:
        typeof evt.data?.reason === "string" && evt.data.reason
          ? `事件流异常：${evt.data.reason}`
          : "事件流异常",
      payload: {
        runId: evt.runId,
        stream: evt.stream,
        seq: evt.seq,
        ts: evt.ts,
        ...evt.data,
      },
      createdAt,
    });
  }
  return null;
}

type PortalSessionRecord = {
  remoteAgentId: string;
  agentId: string;
  sessionKey: string;
  sessionRevision: number;
  turnCount: number;
  historySummary?: string;
  portalSessionId?: string;
  mode: PortalSessionMode;
  conversationView: ControlPlaneConversationView;
  runtimeRole?: ControlPlaneRuntimeRole;
  sessionViews: ControlPlaneConversationView[];
  writePolicy: PortalWritePolicy;
  traceId?: string;
  userContext?: JsonObject;
  createdAt: string;
  updatedAt: string;
  lastRunId?: string;
  agentVersionId?: string;
  skillSnapshotId?: string;
  externalSkillLookupAllowed?: boolean;
  releaseId?: string;
  releaseVersion?: string;
  releaseStatus?: string;
};

type PortalMemoryContext = JsonObject & {
  promptBlock?: string;
  items?: JsonObject[];
};

type PortalMemoryPolicy = JsonObject & {
  runtimeMemory?: string;
  allowRuntimeWrite?: boolean;
  allowUserPrivateWrite?: boolean;
  allowAgentSharedWrite?: boolean;
};

type PortalSkillSearchPrefetch = ControlPlaneSkillSearchResult & {
  externalLookupAllowed: boolean;
};

type PortalRunTimelineItem = {
  phase: "started" | "requires_approval" | "completed" | "failed" | "approval_applied" | "stopped";
  at: string;
  error?: string;
};

type PortalUsage = {
  inputTokens: number;
  outputTokens: number;
  totalTokens: number;
};

type PortalRunRecord = {
  runId: string;
  remoteSessionId: string;
  portalSessionId?: string;
  traceId?: string;
  status: "started" | "requires_approval" | "completed" | "failed" | "approval_applied" | "stopped";
  startedAt: string;
  endedAt?: string;
  durationMs?: number;
  reply?: string;
  streamSeq?: number;
  replyUpdatedAt?: string;
  usage?: PortalUsage;
  error?: {
    message: string;
    code?: string;
  };
  candidateChanges?: PortalCandidateChange[];
  attachments?: PortalMessageAttachment[];
  timeline: PortalRunTimelineItem[];
};

type PortalSharedFileRecord = {
  id: string;
  fileName: string;
  relativePath: string;
  sizeBytes: number;
  mimeType: string;
  updatedAt: string;
  uploadedBy?: string | null;
};

type PortalDeliverablePreviewType = "html" | "image" | "none";

type PortalDeliverableRecord = {
  id: string;
  fileName: string;
  relativePath: string;
  runId: string;
  sizeBytes: number;
  mimeType: string;
  createdAt: string;
  updatedAt: string;
  expiresAt: string;
  previewType: PortalDeliverablePreviewType;
};

type PortalMessageAttachment = {
  id: string;
  kind: "file";
  fileName: string;
  relativePath: string;
  runId: string;
  sizeBytes: number;
  mimeType: string;
  createdAt: string;
  updatedAt: string;
  expiresAt: string;
  previewType: PortalDeliverablePreviewType;
  transport: {
    mode: "managed-download";
  };
};

type ReleaseDescriptor = {
  releaseId?: string;
  releaseVersion?: string;
  releaseStatus?: string;
  releaseManifest?: JsonObject;
  releaseFiles: Array<{ name: string; content: string }>;
};

const PREFIX = "/__control-plane";
const RELEASE_EXPORT_ROOT_FILES = [
  DEFAULT_IDENTITY_FILENAME,
  DEFAULT_AGENTS_FILENAME,
  DEFAULT_MEMORY_FILENAME,
  DEFAULT_MEMORY_ALT_FILENAME,
] as const;
const DEFAULT_PINNED_MEMORY_FILENAME = "memory/pinned.md";
const EMPTY_PORTAL_REPLY = "No response from OpenClaw.";
const PORTAL_HISTORY_SUMMARY_MAX_CHARS = 2_400;
const PORTAL_USER_CONTEXT_MAX_CHARS = 600;
const PORTAL_SESSION_ROLLOVER_TURN_LIMIT = 6;
const PORTAL_SESSION_ROLLOVER_TOKEN_LIMIT = 24_000;
const PLATFORM_DEFAULT_FIND_SKILL_NAME = "find-base-skills";
const PORTAL_SHARED_FILES_DIRNAME = "iqiyi_source";
const PORTAL_SHARED_FILES_MANIFEST = ".portal-files.json";
const PORTAL_SHARED_FILES_PROMPT_LIMIT = 20;
const PORTAL_DELIVERABLES_DIRNAME = "iqiyi_deliverables";
const PORTAL_DELIVERABLE_TTL_MS = 24 * 60 * 60 * 1000;
const PORTAL_DELIVERABLE_CLEANUP_INTERVAL_MS = 60 * 60 * 1000;

const portalSessions = new Map<string, PortalSessionRecord>();
const portalRuns = new Map<string, PortalRunRecord>();
const portalRunAbortControllers = new Map<string, AbortController>();
let portalDeliverablesCleanupTimer: ReturnType<typeof setInterval> | null = null;

function abortTrackedPortalRun(runId: string): {
  aborted: boolean;
  stoppedAt?: string;
  remoteSessionId?: string | null;
  portalSessionId?: string | null;
} {
  const abortController = portalRunAbortControllers.get(runId);
  const existingRun = portalRuns.get(runId);
  const remoteSessionId = existingRun?.remoteSessionId ?? null;
  const portalSessionId = existingRun?.portalSessionId ?? null;
  if (!abortController) {
    return {
      aborted: false,
      remoteSessionId,
      portalSessionId,
    };
  }

  abortController.abort();
  portalRunAbortControllers.delete(runId);

  const stoppedAt = new Date().toISOString();
  if (existingRun && !existingRun.endedAt) {
    savePortalRun({
      ...existingRun,
      status: "stopped",
      endedAt: stoppedAt,
      durationMs: Math.max(0, Date.parse(stoppedAt) - Date.parse(existingRun.startedAt)),
      error: {
        message: "Portal run aborted by user",
        code: "PORTAL_RUN_ABORTED",
      },
      timeline: appendPortalRunTimeline(existingRun, {
        phase: "stopped",
        at: stoppedAt,
        error: "Portal run aborted by user",
      }),
    });
  }

  if (remoteSessionId) {
    const session = portalSessions.get(remoteSessionId);
    if (session) {
      portalSessions.set(remoteSessionId, {
        ...session,
        updatedAt: stoppedAt,
      });
    }
  }

  return {
    aborted: true,
    stoppedAt,
    remoteSessionId,
    portalSessionId,
  };
}

function isJsonObject(value: unknown): value is JsonObject {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function buildPlatformDefaultSkillFiles(): Array<{ name: string; content: string }> {
  return [
    {
      name: `skills/${PLATFORM_DEFAULT_FIND_SKILL_NAME}/SKILL.md`,
      content: `---
name: find-base-skills
description: 先检查当前 workspace 已安装的 skills；本地没有合适 Skill 时，再从当前基座 Skill 仓库搜索、推荐并安装；内部无结果时，先征求用户是否允许外部查询。
---

# Find Base Skills

先检查当前 workspace 已安装的 skills 是否已经满足需求。

- 当前 workspace 已有合适 Skill 时：直接优先使用本地 Skill，不要先跳去内部仓库搜索
- 当前 workspace 没有合适 Skill 时：再使用 \`skill_registry_search\` 搜索当前基座 Skill 仓库
- 找到内部 Skill 时：先推荐候选版本与用途，得到确认后再调用 \`skill_registry_install\`
- 安装内部 Skill 时：不要使用 \`openclaw skills install ...\`、\`npx\`、\`curl\`、\`bash\` 或其他 shell 命令；那样会绕过基座仓库并可能回落到 ClawHub
- 内部 Skill 无结果时：先询问用户是否允许外部查询
- 未获得用户授权前：不要建议 ClawHub、skills.sh、GitHub 等外部 Skill 来源
`,
    },
  ];
}

function looksLikeSkillDiscoveryRequest(message: string): boolean {
  const text = message.trim();
  if (!text) {
    return false;
  }
  return /(skill|技能|技能仓库|skill仓库|查找.*skill|搜索.*skill|推荐.*skill|安装.*skill|下载.*skill|现成.*技能|复用.*技能|find[- ]?skills)/i.test(
    text,
  );
}

function looksLikeExternalSkillLookupApproval(message: string): boolean {
  return /(允许.*外部|可以.*外部|允许.*clawhub|可以.*clawhub|allow.*external|yes.*external|可以去外部查|可以去clawhub查)/i.test(
    message,
  );
}

function looksLikeExternalSkillLookupRevocation(message: string): boolean {
  return /(不要.*外部|不允许.*外部|只查内部|仅查内部|仅从内部|不要去clawhub|不要外部查询)/i.test(
    message,
  );
}

async function prefetchPortalSkillSearch(params: {
  session: PortalSessionRecord;
  message: string;
}): Promise<PortalSkillSearchPrefetch | undefined> {
  if (params.session.mode !== "training" || !looksLikeSkillDiscoveryRequest(params.message)) {
    return undefined;
  }
  try {
    const result = await recommendSkillsFromControlPlane({
      query: params.message,
      limit: 5,
      agentContext: {
        portalSessionId: params.session.portalSessionId ?? null,
        remoteAgentId: params.session.remoteAgentId,
        agentId: params.session.agentId,
        runtimeRole: params.session.runtimeRole ?? null,
        agentVersionId: params.session.agentVersionId ?? null,
        skillSnapshotId: params.session.skillSnapshotId ?? null,
      },
    });
    return {
      ...result,
      externalLookupAllowed: params.session.externalSkillLookupAllowed === true,
    };
  } catch {
    return undefined;
  }
}

function readOptionalString(body: JsonObject, ...keys: string[]): string | undefined {
  for (const key of keys) {
    const value = body[key];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return undefined;
}

function readOptionalBoolean(body: JsonObject, ...keys: string[]): boolean | undefined {
  for (const key of keys) {
    const value = body[key];
    if (typeof value === "boolean") {
      return value;
    }
    if (typeof value === "string") {
      const normalized = value.trim().toLowerCase();
      if (normalized === "true") {
        return true;
      }
      if (normalized === "false") {
        return false;
      }
    }
  }
  return undefined;
}

function readOptionalObject(body: JsonObject, ...keys: string[]): JsonObject | undefined {
  for (const key of keys) {
    const value = body[key];
    if (isJsonObject(value)) {
      return value;
    }
  }
  return undefined;
}

// 受控的递归目录"增量合并"：把 source 下所有常规文件复制到 target 同名路径，
// 同名文件被 source 覆盖，target 里独有的文件保留，source 里独有的文件新增。
// 实现上故意不依赖 Node 的 fs.cp({recursive,force}) ——
//   1) 不同 Node 版本对符号链接 / 跨设备复制 / 已存在空目录 / 与目标同名的 dirent 类型不一致
//      时行为不一致甚至静默忽略；
//   2) 我们需要在每个文件粒度上拿到"被覆盖了什么"的准确信息，以便诊断 skill 丢失问题。
// 跳过符号链接和非常规文件（socket/fifo/blockdev），避免把宿主机上不应被序列化的资源
// 复制到目标 workspace。
async function copyDirectoryTreeIncremental(params: {
  sourceDir: string;
  targetDir: string;
  // 相对于 sourceDir 的相对路径前缀，递归调用时使用。
  relativePrefix?: string;
}): Promise<{
  filesCopied: string[];
  directoriesCreated: string[];
  symlinksSkipped: string[];
  specialSkipped: string[];
}> {
  const filesCopied: string[] = [];
  const directoriesCreated: string[] = [];
  const symlinksSkipped: string[] = [];
  const specialSkipped: string[] = [];
  const prefix = params.relativePrefix ?? "";
  let entries: import("node:fs").Dirent[];
  try {
    entries = await fs.readdir(params.sourceDir, { withFileTypes: true });
  } catch {
    return { filesCopied, directoriesCreated, symlinksSkipped, specialSkipped };
  }
  for (const entry of entries) {
    const sourcePath = path.join(params.sourceDir, entry.name);
    const targetPath = path.join(params.targetDir, entry.name);
    const relativePath = prefix ? `${prefix}/${entry.name}` : entry.name;
    if (entry.isSymbolicLink()) {
      symlinksSkipped.push(relativePath);
      continue;
    }
    if (entry.isDirectory()) {
      await fs.mkdir(targetPath, { recursive: true, mode: 0o700 });
      directoriesCreated.push(relativePath);
      const childResult = await copyDirectoryTreeIncremental({
        sourceDir: sourcePath,
        targetDir: targetPath,
        relativePrefix: relativePath,
      });
      filesCopied.push(...childResult.filesCopied);
      directoriesCreated.push(...childResult.directoriesCreated);
      symlinksSkipped.push(...childResult.symlinksSkipped);
      specialSkipped.push(...childResult.specialSkipped);
      continue;
    }
    if (!entry.isFile()) {
      specialSkipped.push(relativePath);
      continue;
    }
    try {
      const buffer = await fs.readFile(sourcePath);
      await fs.mkdir(path.dirname(targetPath), { recursive: true, mode: 0o700 });
      await fs.writeFile(targetPath, buffer);
      filesCopied.push(relativePath);
    } catch (error) {
      logWarn(
        `control-plane: workspace merge failed to copy ${relativePath} from ${params.sourceDir} -> ${params.targetDir}: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }
  }
  return { filesCopied, directoriesCreated, symlinksSkipped, specialSkipped };
}

// Incremental, non-destructive merge of source workspace tree into target.
// 与 copyWorkspaceTreeIfExists 的区别：不先 fs.rm target —— 同名文件被 source 覆盖，
// target 里独有的文件保留，source 里独有的新增。供"训练 workspace 合并到运行时 workspace"
// 与"v2 候选首次同步从 latest released clone"两条路径共用。
async function mergeWorkspaceTreeIfExists(params: {
  cfg: ReturnType<typeof loadConfig>;
  sourceAgentId: string;
  targetWorkspaceDir: string;
}): Promise<boolean> {
  const sourceWorkspaceDir = resolveAgentWorkspaceDir(params.cfg, params.sourceAgentId);
  if (!(await pathExists(sourceWorkspaceDir))) {
    logInfo(
      `control-plane: skip workspace clone from "${params.sourceAgentId}" -> "${params.targetWorkspaceDir}" because source dir does not exist (${sourceWorkspaceDir})`,
    );
    return false;
  }
  await fs.mkdir(params.targetWorkspaceDir, { recursive: true, mode: 0o700 });
  const result = await copyDirectoryTreeIncremental({
    sourceDir: sourceWorkspaceDir,
    targetDir: params.targetWorkspaceDir,
  });
  logInfo(
    `control-plane: cloned ${result.filesCopied.length} file(s) from ${sourceWorkspaceDir} into ${params.targetWorkspaceDir}` +
      (result.symlinksSkipped.length
        ? ` (skipped ${result.symlinksSkipped.length} symlink(s))`
        : ""),
  );
  return true;
}

async function readJsonFileIfExists(filePath: string): Promise<JsonObject | undefined> {
  try {
    return JSON.parse(await fs.readFile(filePath, "utf8")) as JsonObject;
  } catch {
    return undefined;
  }
}

function mergeTriLayerIndex(
  base: JsonObject | undefined,
  incoming: JsonObject | undefined,
): JsonObject | undefined {
  if (!base && !incoming) {
    return undefined;
  }
  const merged: JsonObject = {
    ...base,
    ...incoming,
  };
  const memoriesByKey = new Map<string, JsonObject>();
  for (const source of [base, incoming]) {
    const memories = Array.isArray(source?.memories) ? source.memories : [];
    for (const item of memories) {
      if (!isJsonObject(item)) {
        continue;
      }
      const id = readOptionalString(item, "id");
      const scope = readOptionalString(item, "scope") ?? "";
      const normalizedSummary = readOptionalString(item, "normalizedSummary") ?? "";
      const userKey = readOptionalString(item, "userKey") ?? "";
      const sessionKey = readOptionalString(item, "sessionKey") ?? "";
      const key = id || `${scope}:${userKey}:${sessionKey}:${normalizedSummary}`;
      if (!key.trim()) {
        continue;
      }
      const previous = memoriesByKey.get(key);
      if (!previous) {
        memoriesByKey.set(key, item);
        continue;
      }
      const previousUpdatedAt = Date.parse(readOptionalString(previous, "updatedAt") ?? "") || 0;
      const incomingUpdatedAt = Date.parse(readOptionalString(item, "updatedAt") ?? "") || 0;
      if (incomingUpdatedAt >= previousUpdatedAt) {
        memoriesByKey.set(key, {
          ...previous,
          ...item,
        });
      }
    }
  }
  merged.memories = [...memoriesByKey.values()];
  merged.updatedAt = new Date().toISOString();
  return merged;
}

async function mergeTriLayerMemoryRoot(params: {
  sourceRoot: string;
  targetRoot: string;
}): Promise<boolean> {
  if (!(await pathExists(params.sourceRoot))) {
    return false;
  }
  const targetIndexBefore = await readJsonFileIfExists(path.join(params.targetRoot, "index.json"));
  const sourceIndex = await readJsonFileIfExists(path.join(params.sourceRoot, "index.json"));
  await fs.mkdir(params.targetRoot, { recursive: true, mode: 0o700 });
  await fs.cp(params.sourceRoot, params.targetRoot, { recursive: true, force: true });
  const mergedIndex = mergeTriLayerIndex(targetIndexBefore, sourceIndex);
  if (mergedIndex) {
    await fs.writeFile(
      path.join(params.targetRoot, "index.json"),
      `${JSON.stringify(mergedIndex, null, 2)}\n`,
      {
        encoding: "utf8",
        mode: 0o600,
      },
    );
  }
  return true;
}

// 把训练 workspace（以及兼容旧契约的 extraSourceLocalAgentKeys 列表）的内容
// 增量合并到目标 workspace。语义：同名文件被 source 覆盖、target 里独有的文件保留、
// source 里独有的新增；最后用控制面提供的"权威 markdown"覆盖一遍这些 markdown。
//
// 关键点：
//   1. 不再先把整个 target workspace 重命名 / 删除，因此用户在 target 里安装的
//      skills/<key>/、iqiyi_source/ 这类运行时产物会保留。
//   2. .tri-layer-memory/tri-layer/index.json 在 fs.cp 之后会被 source 索引覆盖，
//      所以在 cp 前先快照旧 target 索引，cp 后用 mergeTriLayerIndex 把两边条目并起来再写回。
//   3. 旧契约里的 mergeTriLayerMemory.extraSourceLocalAgentKeys（早期把"未稳定的 serving
//      workspace"当 extra source 喂进来）继续支持，避免回退。
async function mergeWorkspaceWithFiles(params: {
  workspaceDir: string;
  files: Array<{ name: string; content: string }>;
  cfg: ReturnType<typeof loadConfig>;
  agentId: string;
  body: JsonObject;
}): Promise<{ mergedSources: string[] }> {
  await fs.mkdir(params.workspaceDir, { recursive: true, mode: 0o700 });
  const ensured = await ensureAgentWorkspace({
    dir: params.workspaceDir,
    // 控制面已经显式给了 workspace 文件清单，OpenClaw 不再自动 seed BOOTSTRAP.md，
    // 否则会和 tri-layer-memory 等插件托管的记忆流冲突。
    ensureBootstrapFiles: false,
  });

  const mergeConfig = readOptionalObject(params.body, "mergeTriLayerMemory", "triLayerMemoryMerge");
  const memoryRootName =
    (mergeConfig ? readOptionalString(mergeConfig, "memoryRootName") : undefined) ??
    readOptionalString(params.body, "memoryRootName") ??
    ".tri-layer-memory";
  const targetMemoryIndexPath = path.join(ensured.dir, memoryRootName, "tri-layer", "index.json");
  const oldTargetMemoryIndex = await readJsonFileIfExists(targetMemoryIndexPath);

  const mergeFromLocalAgentKey = normalizeAgentId(
    readOptionalString(
      params.body,
      "mergeFromLocalAgentKey",
      "mergeFromAgentId",
      "sourceTrainingLocalAgentKey",
      "sourceLocalAgentKey",
    ) ??
      (mergeConfig
        ? readOptionalString(mergeConfig, "sourceLocalAgentKey", "sourceAgentId")
        : undefined) ??
      "",
  );

  const mergedSources: string[] = [];
  const normalizedTargetAgentId = normalizeAgentId(params.agentId);
  if (!mergeFromLocalAgentKey) {
    logInfo(
      `control-plane: workspace merge for agent "${normalizedTargetAgentId}" — no mergeFromLocalAgentKey provided, only platform markdown files will be (over)written; user-installed skills under target are preserved.`,
    );
  } else if (mergeFromLocalAgentKey === normalizedTargetAgentId) {
    logInfo(
      `control-plane: workspace merge for agent "${normalizedTargetAgentId}" — mergeFromLocalAgentKey ("${mergeFromLocalAgentKey}") equals target agentId, source and target are the same workspace; skipping cp and only (over)writing platform markdown files.`,
    );
  } else {
    const sourceWorkspaceDir = resolveAgentWorkspaceDir(params.cfg, mergeFromLocalAgentKey);
    const sourceExists = await pathExists(sourceWorkspaceDir);
    if (!sourceExists) {
      logWarn(
        `control-plane: workspace merge for agent "${normalizedTargetAgentId}" — mergeFromLocalAgentKey "${mergeFromLocalAgentKey}" resolves to ${sourceWorkspaceDir} but that path does not exist on this OpenClaw runtime; skills/files added in the training workspace WILL NOT propagate to the serving workspace.`,
      );
    } else {
      const merge = await copyDirectoryTreeIncremental({
        sourceDir: sourceWorkspaceDir,
        targetDir: ensured.dir,
      });
      mergedSources.push(sourceWorkspaceDir);
      logInfo(
        `control-plane: workspace merge for agent "${normalizedTargetAgentId}" — copied ${merge.filesCopied.length} file(s) from ${sourceWorkspaceDir} into ${ensured.dir}` +
          (merge.symlinksSkipped.length
            ? ` (skipped ${merge.symlinksSkipped.length} symlink(s): ${merge.symlinksSkipped.slice(0, 5).join(", ")})`
            : "") +
          (merge.specialSkipped.length
            ? ` (skipped ${merge.specialSkipped.length} non-regular file(s))`
            : ""),
      );
      if (merge.filesCopied.length === 0) {
        logWarn(
          `control-plane: workspace merge for agent "${normalizedTargetAgentId}" — source workspace ${sourceWorkspaceDir} exists but contains no copyable files; nothing was merged.`,
        );
      }
    }
  }

  // cp 之后 target 的 .tri-layer-memory/tri-layer/index.json 已被 source 覆盖。
  // 用旧 target 索引 + 当前文件（即 source 索引）做 mergeTriLayerIndex 后写回，
  // 防止 target 历史记忆条目被 source 索引挤掉。
  if (mergedSources.length > 0 && oldTargetMemoryIndex) {
    const newSourceMemoryIndex = await readJsonFileIfExists(targetMemoryIndexPath);
    if (newSourceMemoryIndex) {
      const merged = mergeTriLayerIndex(oldTargetMemoryIndex, newSourceMemoryIndex);
      if (merged) {
        await fs.mkdir(path.dirname(targetMemoryIndexPath), {
          recursive: true,
          mode: 0o700,
        });
        await fs.writeFile(targetMemoryIndexPath, `${JSON.stringify(merged, null, 2)}\n`, {
          encoding: "utf8",
          mode: 0o600,
        });
      }
    }
  }

  // 兼容旧契约：mergeTriLayerMemory.extraSourceLocalAgentKeys
  if (mergeConfig) {
    const extraKeys = Array.isArray(mergeConfig.extraSourceLocalAgentKeys)
      ? mergeConfig.extraSourceLocalAgentKeys.filter(
          (value): value is string => typeof value === "string" && value.trim().length > 0,
        )
      : [];
    const targetMemoryRoot = path.dirname(targetMemoryIndexPath);
    for (const key of extraKeys) {
      const extraDir = resolveAgentWorkspaceDir(params.cfg, key);
      const extraMemoryRoot = path.join(extraDir, memoryRootName, "tri-layer");
      if (
        await mergeTriLayerMemoryRoot({
          sourceRoot: extraMemoryRoot,
          targetRoot: targetMemoryRoot,
        })
      ) {
        mergedSources.push(extraMemoryRoot);
      }
    }
  }

  // 最后用控制面提供的"权威 markdown"覆盖一次，确保 AGENTS.md / SOUL.md 等始终是最新版本。
  for (const file of params.files) {
    await writeTextFile(path.join(ensured.dir, file.name), file.content);
  }
  if (params.files.length > 0) {
    logInfo(
      `control-plane: workspace merge for agent "${normalizedTargetAgentId}" — overwrote ${params.files.length} platform markdown file(s) (${params.files.map((f) => f.name).join(", ")}) inside ${ensured.dir}`,
    );
  }

  return { mergedSources };
}

async function clearLocalAgentWorkspace(
  cfg: ReturnType<typeof loadConfig>,
  agentId: string,
): Promise<{ agentId: string; workspaceKey: string } | undefined> {
  const normalizedAgentId = normalizeAgentId(agentId);
  if (!normalizedAgentId) {
    return undefined;
  }
  const workspaceDir = resolveAgentWorkspaceDir(cfg, normalizedAgentId);
  const agentDir = resolveAgentDir(cfg, normalizedAgentId);
  await fs.rm(workspaceDir, { recursive: true, force: true });
  await fs.rm(agentDir, { recursive: true, force: true });
  const currentCfg = loadConfig();
  const remainingEntries = [...listAgentEntries(currentCfg)].filter(
    (entry) => normalizeAgentId(entry.id) !== normalizedAgentId,
  );
  await writeConfigFile({
    ...currentCfg,
    agents: {
      ...currentCfg.agents,
      list: remainingEntries,
    },
  });
  const currentState = loadControlPlaneRuntimeState();
  const remainingRuntimeAgents = (currentState.agents ?? []).filter(
    (entry) => normalizeAgentId(entry.agentId) !== normalizedAgentId,
  );
  mergeControlPlaneRuntimeState({
    agents: remainingRuntimeAgents,
    remoteAgentId:
      remainingRuntimeAgents.length > 0
        ? resolvePrimaryRuntimeRemoteAgentId({
            cfg: loadConfig(),
            currentState,
            agents: remainingRuntimeAgents,
            fallbackRemoteAgentId: remainingRuntimeAgents[0]?.remoteAgentId ?? "",
          })
        : undefined,
  });
  return {
    agentId: normalizedAgentId,
    workspaceKey: path.basename(workspaceDir) || `workspace-${normalizedAgentId}`,
  };
}

function normalizePortalMode(value: unknown): PortalSessionMode {
  return typeof value === "string" && value.trim().toLowerCase() === "training"
    ? "training"
    : "chat";
}

function normalizeRuntimeRole(value: unknown): ControlPlaneRuntimeRole | undefined {
  const normalized = typeof value === "string" ? value.trim().toLowerCase() : "";
  return normalized === "training" || normalized === "serving" ? normalized : undefined;
}

function normalizeRemoteAgentId(value: unknown): string {
  return typeof value === "string" ? value.trim().toLowerCase() : "";
}

function normalizeWorkspaceRelativePath(value: string): string | undefined {
  const trimmed = value
    .trim()
    .replaceAll("\\", "/")
    .replace(/^\.\/+/, "");
  if (!trimmed || trimmed.includes("\0")) {
    return undefined;
  }
  if (trimmed.startsWith("/") || /^[a-zA-Z]:\//.test(trimmed)) {
    return undefined;
  }
  const normalized = path.posix.normalize(trimmed);
  if (!normalized || normalized === "." || normalized.startsWith("../")) {
    return undefined;
  }
  return normalized;
}

function resolveConversationView(mode: PortalSessionMode): ControlPlaneConversationView {
  return mode === "training" ? "training" : "serving";
}

function buildSessionViews(runtimeRole?: ControlPlaneRuntimeRole): ControlPlaneConversationView[] {
  if (runtimeRole === "training") {
    return ["training"];
  }
  if (runtimeRole === "serving") {
    return ["serving"];
  }
  return ["training", "serving"];
}

function buildPortalWritePolicy(conversationView: ControlPlaneConversationView): PortalWritePolicy {
  if (conversationView === "training") {
    return {
      core: "candidate-core",
      memory: "candidate-core",
    };
  }
  return {
    core: "forbidden",
    memory: "user-memory",
  };
}

function buildPortalPluginContext(params: {
  session: PortalSessionRecord;
  portalSessionId?: string;
  traceId?: string;
}) {
  return {
    mode: params.session.mode,
    conversationView: params.session.conversationView,
    runtimeRole: params.session.runtimeRole,
    portalSessionId: params.portalSessionId ?? params.session.portalSessionId,
    traceId: params.traceId ?? params.session.traceId,
    writePolicy: params.session.writePolicy,
    userContext: params.session.userContext,
    releaseId: params.session.releaseId,
    releaseVersion: params.session.releaseVersion,
    releaseStatus: params.session.releaseStatus,
  };
}

function summarizePortalMemoryContextForLog(memoryContext?: PortalMemoryContext) {
  if (!memoryContext) {
    return undefined;
  }
  const items = Array.isArray(memoryContext.items) ? memoryContext.items : [];
  return {
    ...memoryContext,
    itemsCount: items.length,
    items,
    promptBlockPreview:
      typeof memoryContext.promptBlock === "string"
        ? memoryContext.promptBlock.slice(0, 1200)
        : memoryContext.promptBlock,
  };
}

function summarizePortalMemoryPolicyForLog(memoryPolicy?: PortalMemoryPolicy) {
  if (!memoryPolicy) {
    return undefined;
  }
  return {
    runtimeMemory: readOptionalString(memoryPolicy, "runtimeMemory") ?? null,
    allowRuntimeWrite: readOptionalBoolean(memoryPolicy, "allowRuntimeWrite"),
    allowUserPrivateWrite: readOptionalBoolean(memoryPolicy, "allowUserPrivateWrite"),
    allowAgentSharedWrite: readOptionalBoolean(memoryPolicy, "allowAgentSharedWrite"),
  };
}

function buildPortalMemoryPolicyPrompt(memoryPolicy?: PortalMemoryPolicy): string[] {
  if (!memoryPolicy) {
    return [];
  }
  const lines = [
    "## Control-Plane Memory Policy",
    `- runtimeMemory: ${readOptionalString(memoryPolicy, "runtimeMemory") ?? "unspecified"}`,
    `- allowRuntimeWrite: ${readOptionalBoolean(memoryPolicy, "allowRuntimeWrite") === true ? "true" : "false"}`,
    `- allowUserPrivateWrite: ${readOptionalBoolean(memoryPolicy, "allowUserPrivateWrite") === true ? "true" : "false"}`,
    `- allowAgentSharedWrite: ${readOptionalBoolean(memoryPolicy, "allowAgentSharedWrite") === true ? "true" : "false"}`,
  ];
  return lines;
}

function buildPortalMemoryContextPrompt(memoryContext?: PortalMemoryContext): string | undefined {
  const promptBlock =
    typeof memoryContext?.promptBlock === "string" ? memoryContext.promptBlock.trim() : "";
  if (!promptBlock) {
    return undefined;
  }
  return [
    "## Control-Plane Memory Context",
    "The following memory context was supplied by the upstream control-plane for this turn. Treat it as trusted context when answering, but do not invent details beyond what is provided here.",
    promptBlock,
  ].join("\n");
}

function truncateText(value: string | undefined, maxChars: number): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const normalized = value.replace(/\s+/g, " ").trim();
  if (!normalized) {
    return undefined;
  }
  if (normalized.length <= maxChars) {
    return normalized;
  }
  return `${normalized.slice(0, Math.max(0, maxChars - 3)).trimEnd()}...`;
}

function stringifyJson(value: unknown): string | undefined {
  return safeJsonStringify(value) ?? undefined;
}

function buildPortalSessionKey(params: {
  cfg: ReturnType<typeof loadConfig>;
  agentId: string;
  remoteSessionId: string;
  conversationView: ControlPlaneConversationView;
  revision?: number;
}): string {
  const revision =
    typeof params.revision === "number" && Number.isFinite(params.revision) && params.revision > 0
      ? `:r${Math.floor(params.revision)}`
      : "";
  return resolveSessionStoreKey({
    cfg: params.cfg,
    sessionKey: `agent:${normalizeAgentId(params.agentId)}:portal:${params.conversationView}:${params.remoteSessionId.toLowerCase()}${revision}`,
  });
}

function resolvePortalTargetAgent(
  remoteAgentId: string,
  preferredConversationView?: ControlPlaneConversationView,
  preferredLocalAgentKey?: string,
):
  | {
      cfg: ReturnType<typeof loadConfig>;
      agentId: string;
      runtimeState: ReturnType<typeof loadControlPlaneRuntimeState>;
      runtimeAgent?: ControlPlaneRuntimeAgent;
    }
  | undefined {
  const cfg = loadConfig();
  const configuredAgentIds = new Set(listAgentIds(cfg).map((agentId) => normalizeAgentId(agentId)));
  const runtimeState = loadControlPlaneRuntimeState();
  const normalizedRemoteAgentId = normalizeRemoteAgentId(remoteAgentId);

  const matchingAgents = (runtimeState.agents ?? []).filter(
    (entry) => normalizeRemoteAgentId(entry.remoteAgentId) === normalizedRemoteAgentId,
  );

  const preferredLocal = preferredLocalAgentKey ? normalizeAgentId(preferredLocalAgentKey) : "";
  const agentsForRemote = preferredLocal
    ? matchingAgents.filter((entry) => normalizeAgentId(entry.agentId) === preferredLocal)
    : matchingAgents;

  if (preferredLocal && matchingAgents.length > 0 && agentsForRemote.length === 0) {
    return undefined;
  }

  const pickRuntimeAgent = (): ControlPlaneRuntimeAgent | undefined => {
    const pool = agentsForRemote.length > 0 ? agentsForRemote : matchingAgents;
    if (pool.length === 0) {
      return undefined;
    }
    if (pool.length === 1) {
      return pool[0];
    }
    if (preferredConversationView) {
      const byView = pool.find((entry) =>
        (entry.sessionViews ?? buildSessionViews(entry.runtimeRole)).includes(
          preferredConversationView,
        ),
      );
      if (byView) {
        return byView;
      }
    }
    return pool[0];
  };

  const picked = pickRuntimeAgent();
  if (picked) {
    return {
      cfg,
      agentId: normalizeAgentId(picked.agentId),
      runtimeState,
      runtimeAgent: picked,
    };
  }

  if (preferredLocal) {
    return undefined;
  }

  const directAgentId = normalizeAgentId(remoteAgentId);
  if (configuredAgentIds.has(directAgentId)) {
    return { cfg, agentId: directAgentId, runtimeState };
  }

  if (normalizeRemoteAgentId(runtimeState.remoteAgentId) === normalizedRemoteAgentId) {
    return {
      cfg,
      agentId: resolveDefaultAgentId(cfg),
      runtimeState,
    };
  }

  return undefined;
}

function buildPortalExtraSystemPrompt(params: {
  remoteSessionId: string;
  session: PortalSessionRecord;
  traceId?: string;
  portalSessionId?: string;
  runId?: string;
  skillSearchPrefetch?: PortalSkillSearchPrefetch;
  sharedFiles?: PortalSharedFileRecord[];
  memoryContextPrompt?: string;
  memoryPolicy?: PortalMemoryPolicy;
}): string {
  const userContext = truncateText(
    stringifyJson(params.session.userContext),
    PORTAL_USER_CONTEXT_MAX_CHARS,
  );
  const lines = [
    "Portal control-plane session metadata (internal):",
    `- conversationView: ${params.session.conversationView}`,
    `- mode: ${params.session.mode}`,
    params.session.runtimeRole ? `- runtimeRole: ${params.session.runtimeRole}` : undefined,
    params.session.sessionRevision > 0
      ? `- sessionRevision: ${params.session.sessionRevision}`
      : undefined,
    `- writePolicy: core=${params.session.writePolicy.core}; memory=${params.session.writePolicy.memory}`,
    params.portalSessionId || params.session.portalSessionId
      ? `- portalSessionId: ${params.portalSessionId ?? params.session.portalSessionId}`
      : undefined,
    params.traceId || params.session.traceId
      ? `- traceId: ${params.traceId ?? params.session.traceId}`
      : undefined,
    `- remoteSessionId: ${params.remoteSessionId}`,
    params.session.agentVersionId
      ? `- agentVersionId: ${params.session.agentVersionId}`
      : undefined,
    params.session.skillSnapshotId
      ? `- skillSnapshotId: ${params.session.skillSnapshotId}`
      : undefined,
    params.session.releaseVersion
      ? `- releaseVersion: ${params.session.releaseVersion}`
      : undefined,
    params.session.releaseStatus ? `- releaseStatus: ${params.session.releaseStatus}` : undefined,
  ].filter((line): line is string => Boolean(line));

  if (userContext) {
    lines.push("", "## User Context", userContext);
  }
  if (params.memoryContextPrompt) {
    lines.push("", params.memoryContextPrompt);
  }
  if (params.memoryPolicy) {
    lines.push("", ...buildPortalMemoryPolicyPrompt(params.memoryPolicy));
  }
  if (params.session.historySummary) {
    lines.push("", "## Session Memory", params.session.historySummary);
  }
  if ((params.sharedFiles?.length ?? 0) > 0) {
    lines.push(
      "",
      "## Shared Source Files",
      `The current agent workspace includes shared user-uploaded files under ${PORTAL_SHARED_FILES_DIRNAME}/.`,
      "Use these files directly when the task needs workspace data; prefer them over asking the user to re-upload the same content.",
      ...params.sharedFiles!.slice(0, PORTAL_SHARED_FILES_PROMPT_LIMIT).map((file, index) => {
        const parts = [
          `${index + 1}. ${PORTAL_SHARED_FILES_DIRNAME}/${file.relativePath}`,
          `${file.sizeBytes} bytes`,
          file.mimeType,
          file.updatedAt ? `updatedAt=${file.updatedAt}` : undefined,
        ].filter(Boolean);
        return `- ${parts.join(" | ")}`;
      }),
    );
    if ((params.sharedFiles?.length ?? 0) > PORTAL_SHARED_FILES_PROMPT_LIMIT) {
      lines.push(
        `- ...and ${params.sharedFiles!.length - PORTAL_SHARED_FILES_PROMPT_LIMIT} more shared files under ${PORTAL_SHARED_FILES_DIRNAME}/`,
      );
    }
  }
  if (params.runId) {
    const deliverablesSessionId =
      params.portalSessionId ?? params.session.portalSessionId ?? params.remoteSessionId;
    const deliverablesDir = `${PORTAL_DELIVERABLES_DIRNAME}/${normalizePortalDeliverablesSegment(deliverablesSessionId)}/${normalizePortalDeliverablesSegment(params.runId)}/`;
    lines.push(
      "",
      "## Generated Deliverables",
      `If you create files for the user to download, write them under ${deliverablesDir}`,
      "Examples: HTML reports, charts, spreadsheets, slide decks, PDFs, archives, or generated documents.",
      "Prefer self-contained HTML when producing charts or rich visual reports so the portal can preview them directly.",
      "When you generate a downloadable file, mention it explicitly in your reply and include the relative path or filename.",
    );
  }
  if (params.session.conversationView === "training") {
    lines.push(
      "",
      "Training view is enabled. Candidate changes may be proposed as draft runtime state, but nothing is published until the control-plane explicitly approves and releases it.",
      `The workspace already includes the platform skill ${PLATFORM_DEFAULT_FIND_SKILL_NAME}.`,
      "When the user asks for existing capabilities, integrations, or reusable automation, check the currently installed workspace skills first.",
      "Only if the current workspace does not already contain a suitable skill should you search the internal skill registry with skill_registry_search.",
      "Before calling skill_registry_install, summarize the candidate skill and get explicit user confirmation in chat.",
      "Never install internal skills via exec, shell, or `openclaw skills install`; that path may bypass the control-plane registry and fall back to local/default sources such as ClawHub.",
      params.session.externalSkillLookupAllowed === true
        ? "The user has explicitly allowed external skill lookup if the internal registry has no match."
        : "Do not use ClawHub, skills.sh, GitHub skill lists, or any external skill source unless the user explicitly authorizes external lookup in this session.",
    );
    if (params.skillSearchPrefetch) {
      lines.push(
        "",
        "## Internal Skill Registry Prefetch",
        `- query: ${params.skillSearchPrefetch.query}`,
        `- internalMatches: ${params.skillSearchPrefetch.count}`,
        `- externalLookupAllowed: ${params.skillSearchPrefetch.externalLookupAllowed ? "yes" : "no"}`,
      );
      if (params.skillSearchPrefetch.items.length > 0) {
        lines.push(
          ...params.skillSearchPrefetch.items.slice(0, 5).map((item, index) => {
            const parts = [
              `${index + 1}. ${item.name || item.skillKey || "skill"}`,
              item.skillKey ? `skillKey=${item.skillKey}` : undefined,
              item.currentPublishedVersion?.version
                ? `version=${item.currentPublishedVersion.version}`
                : undefined,
              item.summary || item.currentPublishedVersion?.description || undefined,
              item.recommendationReason || undefined,
            ].filter(Boolean);
            return `- ${parts.join(" | ")}`;
          }),
        );
        lines.push(
          "Treat these as fallback internal candidates. Use them only after checking that the current workspace does not already have a suitable installed skill.",
          "If no suitable local skill exists, recommend the best 1-3 internal candidates and ask the user to confirm a specific skill/version before installation.",
        );
      } else if (params.skillSearchPrefetch.externalLookupAllowed) {
        lines.push(
          "The internal registry returned no match. After confirming no suitable local workspace skill exists, tell the user that no internal skill was found; external skill lookup is now allowed if it helps.",
        );
      } else {
        lines.push(
          "The internal registry returned no match. After confirming no suitable local workspace skill exists, do not query external skill sources yet. Reply that no internal skill was found and ask whether the user allows external skill lookup.",
        );
      }
    }
  } else {
    lines.push(
      "",
      "Serving view is enabled. Never mutate published core instructions or release definitions from this conversation.",
    );
  }

  return lines.join("\n");
}

function resolvePortalReplyText(result: unknown): string {
  const payloads = (result as { payloads?: Array<{ text?: string }> } | null)?.payloads;
  if (!Array.isArray(payloads) || payloads.length === 0) {
    return EMPTY_PORTAL_REPLY;
  }
  const reply = payloads
    .map((payload) => (typeof payload.text === "string" ? payload.text : ""))
    .filter(Boolean)
    .join("\n\n")
    .trim();
  return reply || EMPTY_PORTAL_REPLY;
}

function extractPortalUsage(result: unknown): PortalUsage {
  const usage = ((result as { meta?: { agentMeta?: { usage?: unknown } } } | null)?.meta?.agentMeta
    ?.usage as
    | {
        input?: number;
        output?: number;
        cacheRead?: number;
        cacheWrite?: number;
        total?: number;
      }
    | undefined) ?? { total: 0 };
  const input = usage.input ?? 0;
  const output = usage.output ?? 0;
  const total = usage.total ?? input + output + (usage.cacheRead ?? 0) + (usage.cacheWrite ?? 0);
  return {
    inputTokens: Math.max(0, input),
    outputTokens: Math.max(0, output),
    totalTokens: Math.max(0, total),
  };
}

function inferCandidateRiskLevel(params: {
  message: string;
  reply: string;
  approval?: PortalApprovalSummary;
}): PortalCandidateRiskLevel {
  if (params.approval) {
    return "high";
  }
  const combined = `${params.message}\n${params.reply}`.toLowerCase();
  if (/(deploy|publish|release|exec|shell|command|delete|drop|shutdown|migrate)/.test(combined)) {
    return "high";
  }
  if (/(update|change|edit|modify|memory|policy|workflow|prompt|config)/.test(combined)) {
    return "medium";
  }
  return "low";
}

function buildCandidateDiff(
  currentValue: string | null,
  proposedValue: string | null,
): string | null {
  const parts: string[] = [];
  if (currentValue) {
    parts.push(`Current\n- ${currentValue}`);
  }
  if (proposedValue) {
    parts.push(`Proposed\n+ ${proposedValue}`);
  }
  return parts.length > 0 ? parts.join("\n\n") : null;
}

function buildTrainingCandidateChanges(params: {
  session: PortalSessionRecord;
  message: string;
  reply: string;
  status: "completed" | "requires_approval";
  approval?: PortalApprovalSummary;
}): PortalCandidateChange[] | undefined {
  if (params.session.mode !== "training") {
    return undefined;
  }
  const currentValue = truncateText(params.message, 240) ?? null;
  const proposedValue =
    params.reply === EMPTY_PORTAL_REPLY ? null : (truncateText(params.reply, 320) ?? null);
  const changes: PortalCandidateChange[] = [
    {
      kind: "conversation-summary",
      title: "Training conversation summary",
      summary:
        params.status === "requires_approval"
          ? "Runtime paused the training flow because a command requires approval."
          : "Runtime generated a deterministic training summary from the portal exchange.",
      currentValue,
      proposedValue,
      diffText: buildCandidateDiff(currentValue, proposedValue),
      riskLevel: inferCandidateRiskLevel(params),
      metadata: {
        source: "control-plane-http",
        agentId: params.session.agentId,
        remoteAgentId: params.session.remoteAgentId,
        mode: params.session.mode,
        conversationView: params.session.conversationView,
        runtimeRole: params.session.runtimeRole ?? null,
        responseStatus: params.status,
      },
    },
  ];
  if (params.approval) {
    const approvalCommand = truncateText(params.approval.command, 240) ?? params.approval.command;
    changes.push({
      kind: "exec-approval",
      title: "Exec approval required",
      summary:
        "The runtime paused before executing a command requested during the training workflow.",
      currentValue: "Awaiting explicit control-plane approval.",
      proposedValue: approvalCommand,
      diffText: buildCandidateDiff("Awaiting explicit control-plane approval.", approvalCommand),
      riskLevel: "high",
      metadata: {
        source: "control-plane-http",
        approvalId: params.approval.id,
        approvalKind: params.approval.kind,
        host: params.approval.host ?? null,
        cwd: params.approval.cwd ?? null,
        expiresAt: params.approval.expiresAt ?? null,
      },
    });
  }
  return changes;
}

function appendPortalRunTimeline(
  existing: PortalRunRecord | undefined,
  item: PortalRunTimelineItem,
): PortalRunTimelineItem[] {
  return [...(existing?.timeline ?? []), item];
}

function savePortalRun(record: PortalRunRecord): PortalRunRecord {
  portalRuns.set(record.runId, record);
  return record;
}

function summarizePortalExchange(params: {
  message: string;
  reply: string;
  usage: PortalUsage;
}): string {
  const message = truncateText(params.message, 220) ?? "(empty)";
  const reply =
    params.reply === EMPTY_PORTAL_REPLY
      ? "No visible assistant reply."
      : (truncateText(params.reply, 320) ?? "No visible assistant reply.");
  return [
    `User: ${message}`,
    `Assistant: ${reply}`,
    `Usage: input=${params.usage.inputTokens}, output=${params.usage.outputTokens}, total=${params.usage.totalTokens}`,
  ].join("\n");
}

function appendHistorySummary(currentSummary: string | undefined, exchangeSummary: string): string {
  const merged = [currentSummary?.trim(), exchangeSummary.trim()].filter(Boolean).join("\n\n");
  if (merged.length <= PORTAL_HISTORY_SUMMARY_MAX_CHARS) {
    return merged;
  }
  return `...${merged.slice(-(PORTAL_HISTORY_SUMMARY_MAX_CHARS - 3)).trimStart()}`;
}

function shouldRolloverPortalSession(params: {
  session: PortalSessionRecord;
  usage: PortalUsage;
}): boolean {
  return (
    params.session.turnCount >= PORTAL_SESSION_ROLLOVER_TURN_LIMIT ||
    params.usage.totalTokens >= PORTAL_SESSION_ROLLOVER_TOKEN_LIMIT
  );
}

function sendJson(res: ServerResponse, status: number, body: unknown): void {
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.setHeader("Cache-Control", "no-store");
  res.end(safeJsonStringify(body) ?? '{"ok":false,"error":"serialization failure"}');
}

async function readBody(req: IncomingMessage): Promise<JsonObject> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    chunks.push(typeof chunk === "string" ? Buffer.from(chunk) : chunk);
  }
  if (chunks.length === 0) {
    return {};
  }
  const text = Buffer.concat(chunks).toString("utf-8").trim();
  if (!text) {
    return {};
  }
  const parsed = JSON.parse(text);
  return isJsonObject(parsed) ? parsed : {};
}

function authorizeBridge(req: IncomingMessage): boolean {
  const expected = process.env.OPENCLAW_BRIDGE_TOKEN?.trim();
  if (!expected) {
    return true;
  }
  const header = String(req.headers["x-openclaw-bridge-token"] ?? "").trim();
  return header === expected;
}

function ensureMethod(
  req: IncomingMessage,
  res: ServerResponse,
  allowed: string | string[],
): boolean {
  const method = (req.method ?? "GET").toUpperCase();
  const allowedList = Array.isArray(allowed) ? allowed : [allowed];
  if (allowedList.includes(method)) {
    return true;
  }
  res.statusCode = 405;
  res.setHeader("Allow", allowedList.join(", "));
  res.end("Method Not Allowed");
  return false;
}

function parseWorkspaceFilesFromValue(value: unknown): Array<{ name: string; content: string }> {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .filter(
      (item): item is { name?: unknown; content?: unknown } =>
        Boolean(item) && typeof item === "object" && !Array.isArray(item),
    )
    .map((item) => ({
      name: typeof item.name === "string" ? (normalizeWorkspaceRelativePath(item.name) ?? "") : "",
      content: typeof item.content === "string" ? item.content : "",
    }))
    .filter((item) => item.name && item.content);
}

function workspaceFilesInclude(files: Array<{ name: string }>, name: string): boolean {
  return files.some((file) => file.name === name);
}

function parseReleaseDescriptor(body: JsonObject): ReleaseDescriptor {
  const release = readOptionalObject(body, "release", "releaseBundle");
  const topLevelReleaseFiles = parseWorkspaceFilesFromValue(body.releaseFiles);
  const nestedReleaseFiles = parseWorkspaceFilesFromValue(release?.files);
  const artifacts = release ? readOptionalObject(release, "artifacts") : undefined;
  const artifactWorkspaceFiles = artifacts
    ? parseWorkspaceFilesFromValue(artifacts.workspaceFiles)
    : [];
  const releaseFiles =
    topLevelReleaseFiles.length > 0
      ? topLevelReleaseFiles
      : nestedReleaseFiles.length > 0
        ? nestedReleaseFiles
        : artifactWorkspaceFiles;
  return {
    releaseId:
      readOptionalString(body, "releaseId") ??
      (release ? readOptionalString(release, "releaseId", "id") : undefined),
    releaseVersion:
      readOptionalString(body, "releaseVersion") ??
      (release ? readOptionalString(release, "releaseVersion", "version") : undefined),
    releaseStatus:
      readOptionalString(body, "releaseStatus") ??
      (release ? readOptionalString(release, "releaseStatus", "status") : undefined),
    releaseManifest:
      readOptionalObject(body, "releaseManifest") ??
      (release ? readOptionalObject(release, "manifest") : undefined),
    releaseFiles,
  };
}

async function pathExists(targetPath: string): Promise<boolean> {
  try {
    await fs.access(targetPath);
    return true;
  } catch {
    return false;
  }
}

async function writeTextFile(targetPath: string, content: string): Promise<void> {
  await fs.mkdir(path.dirname(targetPath), { recursive: true, mode: 0o700 });
  await fs.writeFile(targetPath, `${content.trimEnd()}\n`, { encoding: "utf-8", mode: 0o600 });
}

function resolvePortalSharedFilesRoot(workspaceDir: string): string {
  return path.join(workspaceDir, PORTAL_SHARED_FILES_DIRNAME);
}

function resolvePortalSharedFilesManifestPath(workspaceDir: string): string {
  return path.join(resolvePortalSharedFilesRoot(workspaceDir), PORTAL_SHARED_FILES_MANIFEST);
}

function normalizePortalSharedRelativePath(value: string): string | undefined {
  const normalized = normalizeWorkspaceRelativePath(value);
  if (!normalized) {
    return undefined;
  }
  if (
    normalized === PORTAL_SHARED_FILES_MANIFEST ||
    normalized.startsWith(`${PORTAL_SHARED_FILES_MANIFEST}/`)
  ) {
    return undefined;
  }
  return normalized;
}

function resolvePortalSharedFilePath(workspaceDir: string, relativePath: string): string {
  const root = resolvePortalSharedFilesRoot(workspaceDir);
  const target = path.join(root, relativePath);
  if (!isPathInside(root, target)) {
    throw new Error("shared file path escapes iqiyi_source root");
  }
  return target;
}

async function loadPortalSharedFilesManifest(
  workspaceDir: string,
): Promise<PortalSharedFileRecord[]> {
  const manifestPath = resolvePortalSharedFilesManifestPath(workspaceDir);
  try {
    const raw = await fs.readFile(manifestPath, "utf-8");
    const parsed = JSON.parse(raw) as unknown;
    if (!Array.isArray(parsed)) {
      return [];
    }
    return parsed
      .filter(
        (item): item is PortalSharedFileRecord =>
          Boolean(item) &&
          typeof item === "object" &&
          typeof (item as PortalSharedFileRecord).relativePath === "string" &&
          typeof (item as PortalSharedFileRecord).fileName === "string",
      )
      .map((item) => ({
        id: item.relativePath,
        fileName: item.fileName,
        relativePath: item.relativePath,
        sizeBytes: Number.isFinite(item.sizeBytes) ? item.sizeBytes : 0,
        mimeType: item.mimeType || "application/octet-stream",
        updatedAt: item.updatedAt || new Date().toISOString(),
        uploadedBy: item.uploadedBy ?? null,
      }));
  } catch {
    return [];
  }
}

async function savePortalSharedFilesManifest(
  workspaceDir: string,
  records: PortalSharedFileRecord[],
): Promise<void> {
  const root = resolvePortalSharedFilesRoot(workspaceDir);
  await fs.mkdir(root, { recursive: true, mode: 0o700 });
  const manifestPath = resolvePortalSharedFilesManifestPath(workspaceDir);
  await fs.writeFile(manifestPath, `${JSON.stringify(records, null, 2)}\n`, {
    encoding: "utf-8",
    mode: 0o600,
  });
}

async function listPortalSharedFiles(workspaceDir: string): Promise<PortalSharedFileRecord[]> {
  const manifest = await loadPortalSharedFilesManifest(workspaceDir);
  const next: PortalSharedFileRecord[] = [];
  let changed = false;
  for (const record of manifest) {
    const normalized = normalizePortalSharedRelativePath(record.relativePath);
    if (!normalized) {
      changed = true;
      continue;
    }
    try {
      const targetPath = resolvePortalSharedFilePath(workspaceDir, normalized);
      const stat = await fs.stat(targetPath);
      if (!stat.isFile()) {
        changed = true;
        continue;
      }
      next.push({
        ...record,
        id: normalized,
        fileName: path.posix.basename(normalized),
        relativePath: normalized,
        sizeBytes: stat.size,
      });
    } catch {
      changed = true;
    }
  }
  next.sort((a, b) => a.relativePath.localeCompare(b.relativePath));
  if (changed) {
    await savePortalSharedFilesManifest(workspaceDir, next);
  }
  return next;
}

async function upsertPortalSharedFiles(params: {
  workspaceDir: string;
  files: Array<{
    fileName?: string;
    relativePath?: string;
    mimeType?: string;
    contentBase64?: string;
  }>;
  overwrite?: boolean;
  uploadedBy?: string | null;
}): Promise<PortalSharedFileRecord[]> {
  const root = resolvePortalSharedFilesRoot(params.workspaceDir);
  await fs.mkdir(root, { recursive: true, mode: 0o700 });
  const overwrite = params.overwrite !== false;
  const manifest = await loadPortalSharedFilesManifest(params.workspaceDir);
  const recordMap = new Map(manifest.map((item) => [item.relativePath, item]));
  const uploaded: PortalSharedFileRecord[] = [];

  for (const file of params.files) {
    const inputPath = readOptionalString(file as JsonObject, "relativePath", "fileName");
    const relativePath = inputPath ? normalizePortalSharedRelativePath(inputPath) : undefined;
    if (!relativePath) {
      throw new Error("missing or invalid relativePath");
    }
    const contentBase64 = readOptionalString(file as JsonObject, "contentBase64");
    if (!contentBase64) {
      throw new Error(`missing contentBase64 for ${relativePath}`);
    }
    const targetPath = resolvePortalSharedFilePath(params.workspaceDir, relativePath);
    if (!overwrite && (await pathExists(targetPath))) {
      throw new Error(`shared file already exists: ${relativePath}`);
    }
    const buffer = Buffer.from(contentBase64, "base64");
    const now = new Date().toISOString();
    const tempPath = `${targetPath}.tmp-${randomUUID().replace(/-/g, "")}`;
    await fs.mkdir(path.dirname(targetPath), { recursive: true, mode: 0o700 });
    await fs.writeFile(tempPath, buffer, { mode: 0o600 });
    await fs.rename(tempPath, targetPath);
    const stat = await fs.stat(targetPath);
    const record: PortalSharedFileRecord = {
      id: relativePath,
      fileName: path.posix.basename(relativePath),
      relativePath,
      sizeBytes: stat.size,
      mimeType: readOptionalString(file as JsonObject, "mimeType") ?? "application/octet-stream",
      updatedAt: now,
      uploadedBy: params.uploadedBy ?? null,
    };
    recordMap.set(relativePath, record);
    uploaded.push(record);
  }

  const next = [...recordMap.values()].toSorted((a, b) =>
    a.relativePath.localeCompare(b.relativePath),
  );
  await savePortalSharedFilesManifest(params.workspaceDir, next);
  return uploaded;
}

async function deletePortalSharedFile(
  workspaceDir: string,
  fileIdOrRelativePath: string,
): Promise<PortalSharedFileRecord | null> {
  const relativePath = normalizePortalSharedRelativePath(fileIdOrRelativePath);
  if (!relativePath) {
    throw new Error("missing or invalid shared file id");
  }
  const manifest = await loadPortalSharedFilesManifest(workspaceDir);
  const current = manifest.find((item) => item.relativePath === relativePath) ?? null;
  const targetPath = resolvePortalSharedFilePath(workspaceDir, relativePath);
  await fs.rm(targetPath, { force: true });
  const next = manifest.filter((item) => item.relativePath !== relativePath);
  await savePortalSharedFilesManifest(workspaceDir, next);
  return current;
}

function normalizePortalDeliverablesSegment(value: string | undefined | null): string {
  const trimmed = (value ?? "").trim();
  return trimmed.replace(/[^a-zA-Z0-9._-]+/g, "_") || "default";
}

function resolvePortalDeliverablesRoot(workspaceDir: string): string {
  return path.join(workspaceDir, PORTAL_DELIVERABLES_DIRNAME);
}

function resolvePortalSessionDeliverablesRoot(
  workspaceDir: string,
  portalSessionId: string,
): string {
  return path.join(
    resolvePortalDeliverablesRoot(workspaceDir),
    normalizePortalDeliverablesSegment(portalSessionId),
  );
}

function resolvePortalRunDeliverablesRoot(
  workspaceDir: string,
  portalSessionId: string,
  runId: string,
): string {
  return path.join(
    resolvePortalSessionDeliverablesRoot(workspaceDir, portalSessionId),
    normalizePortalDeliverablesSegment(runId),
  );
}

function normalizePortalDeliverableRelativePath(value: string): string | undefined {
  const normalized = value.replaceAll("\\", "/").trim();
  if (!normalized) {
    return undefined;
  }
  const next = path.posix.normalize(normalized).replace(/^\/+/, "");
  if (!next || next === "." || next.startsWith("../") || next.includes("/../")) {
    return undefined;
  }
  return next;
}

function resolvePortalDeliverablePath(
  workspaceDir: string,
  portalSessionId: string,
  artifactId: string,
): string {
  const sessionRoot = resolvePortalSessionDeliverablesRoot(workspaceDir, portalSessionId);
  const relativePath = normalizePortalDeliverableRelativePath(artifactId);
  if (!relativePath) {
    throw new Error("missing or invalid deliverable id");
  }
  const target = path.join(sessionRoot, relativePath);
  if (!isPathInside(sessionRoot, target)) {
    throw new Error("deliverable path escapes iqiyi_deliverables root");
  }
  return target;
}

function inferPortalDeliverableMimeType(fileName: string): string {
  const lower = fileName.toLowerCase();
  if (lower.endsWith(".html") || lower.endsWith(".htm")) {
    return "text/html; charset=utf-8";
  }
  if (lower.endsWith(".json")) {
    return "application/json; charset=utf-8";
  }
  if (lower.endsWith(".md")) {
    return "text/markdown; charset=utf-8";
  }
  if (lower.endsWith(".txt")) {
    return "text/plain; charset=utf-8";
  }
  if (lower.endsWith(".csv")) {
    return "text/csv; charset=utf-8";
  }
  if (lower.endsWith(".svg")) {
    return "image/svg+xml";
  }
  if (lower.endsWith(".png")) {
    return "image/png";
  }
  if (lower.endsWith(".jpg") || lower.endsWith(".jpeg")) {
    return "image/jpeg";
  }
  if (lower.endsWith(".gif")) {
    return "image/gif";
  }
  if (lower.endsWith(".webp")) {
    return "image/webp";
  }
  if (lower.endsWith(".pdf")) {
    return "application/pdf";
  }
  if (lower.endsWith(".docx")) {
    return "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
  }
  if (lower.endsWith(".xlsx")) {
    return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
  }
  if (lower.endsWith(".pptx")) {
    return "application/vnd.openxmlformats-officedocument.presentationml.presentation";
  }
  if (lower.endsWith(".zip")) {
    return "application/zip";
  }
  return "application/octet-stream";
}

function inferPortalDeliverablePreviewType(fileName: string): PortalDeliverablePreviewType {
  const lower = fileName.toLowerCase();
  if (lower.endsWith(".html") || lower.endsWith(".htm")) {
    return "html";
  }
  if (
    lower.endsWith(".png") ||
    lower.endsWith(".jpg") ||
    lower.endsWith(".jpeg") ||
    lower.endsWith(".gif") ||
    lower.endsWith(".webp") ||
    lower.endsWith(".svg")
  ) {
    return "image";
  }
  return "none";
}

async function collectFilesRecursively(rootDir: string, currentDir = rootDir): Promise<string[]> {
  const entries = await fs.readdir(currentDir, { withFileTypes: true });
  const files: string[] = [];
  for (const entry of entries) {
    const absolutePath = path.join(currentDir, entry.name);
    if (entry.isDirectory()) {
      files.push(...(await collectFilesRecursively(rootDir, absolutePath)));
      continue;
    }
    if (!entry.isFile()) {
      continue;
    }
    const relative = path.relative(rootDir, absolutePath).replaceAll(path.sep, "/");
    const normalized = normalizePortalDeliverableRelativePath(relative);
    if (normalized) {
      files.push(normalized);
    }
  }
  return files;
}

function buildPortalDeliverableRecord(params: {
  relativePath: string;
  stat: Awaited<ReturnType<typeof fs.stat>>;
}): PortalDeliverableRecord {
  const relativePath = params.relativePath.replaceAll("\\", "/");
  const [runId = ""] = relativePath.split("/");
  const fileName = path.posix.basename(relativePath);
  const createdAt = params.stat.birthtime.toISOString();
  const updatedAt = params.stat.mtime.toISOString();
  const sizeBytes = Number(params.stat.size);
  const updatedAtMs = Number(params.stat.mtimeMs);
  return {
    id: relativePath,
    fileName,
    relativePath,
    runId,
    sizeBytes,
    mimeType: inferPortalDeliverableMimeType(fileName),
    createdAt,
    updatedAt,
    expiresAt: new Date(updatedAtMs + PORTAL_DELIVERABLE_TTL_MS).toISOString(),
    previewType: inferPortalDeliverablePreviewType(fileName),
  };
}

function isPortalDeliverableExpired(stat: Awaited<ReturnType<typeof fs.stat>>): boolean {
  return Number(stat.mtimeMs) + PORTAL_DELIVERABLE_TTL_MS <= Date.now();
}

async function pruneEmptyDirectory(targetDir: string, stopDir: string): Promise<void> {
  let current = targetDir;
  while (current === stopDir || isPathInside(stopDir, current)) {
    const entries = await fs.readdir(current).catch(() => []);
    if (entries.length > 0) {
      return;
    }
    await fs.rm(current, { recursive: true, force: true }).catch(() => {});
    if (current === stopDir) {
      return;
    }
    current = path.dirname(current);
  }
}

async function cleanupExpiredPortalDeliverablesForSession(
  workspaceDir: string,
  portalSessionId: string,
): Promise<void> {
  const sessionRoot = resolvePortalSessionDeliverablesRoot(workspaceDir, portalSessionId);
  if (!(await pathExists(sessionRoot))) {
    return;
  }
  const relativeFiles = await collectFilesRecursively(sessionRoot).catch(() => []);
  for (const relativePath of relativeFiles) {
    try {
      const absolutePath = resolvePortalDeliverablePath(
        workspaceDir,
        portalSessionId,
        relativePath,
      );
      const stat = await fs.stat(absolutePath);
      if (!stat.isFile() || !isPortalDeliverableExpired(stat)) {
        continue;
      }
      await fs.rm(absolutePath, { force: true });
      await pruneEmptyDirectory(path.dirname(absolutePath), sessionRoot);
    } catch {
      // best effort cleanup
    }
  }
}

async function cleanupExpiredPortalDeliverablesInWorkspace(workspaceDir: string): Promise<void> {
  const root = resolvePortalDeliverablesRoot(workspaceDir);
  if (!(await pathExists(root))) {
    return;
  }
  const sessions = await fs.readdir(root, { withFileTypes: true }).catch(() => []);
  for (const entry of sessions) {
    if (!entry.isDirectory()) {
      continue;
    }
    await cleanupExpiredPortalDeliverablesForSession(workspaceDir, entry.name);
  }
}

async function cleanupExpiredPortalDeliverablesAcrossAgents(): Promise<void> {
  const cfg = loadConfig();
  for (const agentId of listAgentIds(cfg)) {
    await cleanupExpiredPortalDeliverablesInWorkspace(resolveAgentWorkspaceDir(cfg, agentId));
  }
}

function ensurePortalDeliverablesCleanupTimerStarted(): void {
  if (portalDeliverablesCleanupTimer) {
    return;
  }
  portalDeliverablesCleanupTimer = setInterval(() => {
    void cleanupExpiredPortalDeliverablesAcrossAgents().catch(() => {});
  }, PORTAL_DELIVERABLE_CLEANUP_INTERVAL_MS);
  if (typeof portalDeliverablesCleanupTimer.unref === "function") {
    portalDeliverablesCleanupTimer.unref();
  }
  void cleanupExpiredPortalDeliverablesAcrossAgents().catch(() => {});
}

async function listPortalDeliverablesForSession(
  workspaceDir: string,
  portalSessionId: string,
): Promise<PortalDeliverableRecord[]> {
  await cleanupExpiredPortalDeliverablesForSession(workspaceDir, portalSessionId);
  const sessionRoot = resolvePortalSessionDeliverablesRoot(workspaceDir, portalSessionId);
  if (!(await pathExists(sessionRoot))) {
    return [];
  }
  const relativeFiles = await collectFilesRecursively(sessionRoot);
  const records: PortalDeliverableRecord[] = [];
  for (const relativePath of relativeFiles) {
    const absolutePath = resolvePortalDeliverablePath(workspaceDir, portalSessionId, relativePath);
    const stat = await fs.stat(absolutePath).catch(() => null);
    if (!stat?.isFile()) {
      continue;
    }
    records.push(
      buildPortalDeliverableRecord({
        relativePath,
        stat,
      }),
    );
  }
  return records.toSorted((a, b) => b.updatedAt.localeCompare(a.updatedAt));
}

function buildPortalMessageAttachments(
  deliverables: PortalDeliverableRecord[],
): PortalMessageAttachment[] {
  return deliverables.map((deliverable) => ({
    id: deliverable.id,
    kind: "file",
    fileName: deliverable.fileName,
    relativePath: deliverable.relativePath,
    runId: deliverable.runId,
    sizeBytes: deliverable.sizeBytes,
    mimeType: deliverable.mimeType,
    createdAt: deliverable.createdAt,
    updatedAt: deliverable.updatedAt,
    expiresAt: deliverable.expiresAt,
    previewType: deliverable.previewType,
    transport: {
      mode: "managed-download",
    },
  }));
}

function collectPortalReplyReferencedDeliverables(params: {
  deliverables: PortalDeliverableRecord[];
  portalSessionId: string;
  reply?: string;
}): PortalDeliverableRecord[] {
  const reply = params.reply?.trim();
  if (!reply) {
    return [];
  }
  const normalizedReply = reply.toLowerCase();
  const basenameCounts = new Map<string, number>();
  for (const deliverable of params.deliverables) {
    const basename = deliverable.fileName.toLowerCase();
    basenameCounts.set(basename, (basenameCounts.get(basename) ?? 0) + 1);
  }
  return params.deliverables.filter((deliverable) => {
    const relativePath = deliverable.relativePath.toLowerCase();
    const basename = deliverable.fileName.toLowerCase();
    const fullRelativePath =
      `${PORTAL_DELIVERABLES_DIRNAME}/${normalizePortalDeliverablesSegment(params.portalSessionId)}/${deliverable.relativePath}`.toLowerCase();
    return (
      normalizedReply.includes(fullRelativePath) ||
      normalizedReply.includes(relativePath) ||
      ((basenameCounts.get(basename) ?? 0) === 1 && normalizedReply.includes(basename))
    );
  });
}

async function resolvePortalMessageAttachments(params: {
  workspaceDir: string;
  portalSessionId: string;
  runId: string;
  reply?: string;
}): Promise<PortalMessageAttachment[]> {
  const deliverables = await listPortalDeliverablesForSession(
    params.workspaceDir,
    params.portalSessionId,
  );
  const currentRunDeliverables = deliverables.filter(
    (deliverable) => deliverable.runId === params.runId,
  );
  const referencedDeliverables = collectPortalReplyReferencedDeliverables({
    deliverables,
    portalSessionId: params.portalSessionId,
    reply: params.reply,
  });
  const merged = new Map<string, PortalDeliverableRecord>();
  for (const deliverable of [...currentRunDeliverables, ...referencedDeliverables]) {
    merged.set(deliverable.id, deliverable);
  }
  return buildPortalMessageAttachments([...merged.values()]);
}

async function prunePortalRunDeliverablesRoot(
  workspaceDir: string,
  portalSessionId: string,
  runId: string,
): Promise<void> {
  await pruneEmptyDirectory(
    resolvePortalRunDeliverablesRoot(workspaceDir, portalSessionId, runId),
    resolvePortalSessionDeliverablesRoot(workspaceDir, portalSessionId),
  );
}

async function readPortalDeliverable(
  workspaceDir: string,
  portalSessionId: string,
  artifactId: string,
): Promise<{ record: PortalDeliverableRecord; content: Buffer }> {
  await cleanupExpiredPortalDeliverablesForSession(workspaceDir, portalSessionId);
  const targetPath = resolvePortalDeliverablePath(workspaceDir, portalSessionId, artifactId);
  const stat = await fs.stat(targetPath);
  if (!stat.isFile()) {
    throw new Error("deliverable not found");
  }
  if (isPortalDeliverableExpired(stat)) {
    await fs.rm(targetPath, { force: true }).catch(() => {});
    throw new Error("deliverable expired");
  }
  const relativePath = normalizePortalDeliverableRelativePath(artifactId);
  if (!relativePath) {
    throw new Error("missing or invalid deliverable id");
  }
  return {
    record: buildPortalDeliverableRecord({ relativePath, stat }),
    content: await fs.readFile(targetPath),
  };
}

async function deletePortalDeliverable(
  workspaceDir: string,
  portalSessionId: string,
  artifactId: string,
): Promise<PortalDeliverableRecord | null> {
  const relativePath = normalizePortalDeliverableRelativePath(artifactId);
  if (!relativePath) {
    throw new Error("missing or invalid deliverable id");
  }
  const targetPath = resolvePortalDeliverablePath(workspaceDir, portalSessionId, relativePath);
  const stat = await fs.stat(targetPath).catch(() => null);
  const record =
    stat?.isFile() === true
      ? buildPortalDeliverableRecord({
          relativePath,
          stat,
        })
      : null;
  await fs.rm(targetPath, { force: true });
  await pruneEmptyDirectory(
    path.dirname(targetPath),
    resolvePortalSessionDeliverablesRoot(workspaceDir, portalSessionId),
  );
  return record;
}

async function copyFileIfMissing(sourcePath: string, targetPath: string): Promise<void> {
  if (!(await pathExists(sourcePath)) || (await pathExists(targetPath))) {
    return;
  }
  await fs.mkdir(path.dirname(targetPath), { recursive: true, mode: 0o700 });
  await fs.copyFile(sourcePath, targetPath);
  try {
    await fs.chmod(targetPath, 0o600);
  } catch {
    // best effort
  }
}

function buildDefaultWorkspaceFiles(params: {
  agentId: string;
  name?: string;
  description?: string;
  systemPrompt?: string;
  skillSnapshotId?: string;
}): Array<{ name: string; content: string }> {
  const identityLines = [
    "# IDENTITY.md - Agent Identity",
    "",
    `- Name: ${params.name || params.agentId}`,
    "- Creature:",
    "- Vibe:",
    "- Emoji:",
  ];
  const promptLines = [
    "# AGENTS.md - Synced Agent Instructions",
    "",
    `- Agent ID: ${params.agentId}`,
    params.name ? `- Name: ${params.name}` : undefined,
    params.description ? `- Description: ${params.description}` : undefined,
    params.skillSnapshotId ? `- Skill Snapshot: ${params.skillSnapshotId}` : undefined,
    "",
    params.systemPrompt ? "## System Prompt" : undefined,
    params.systemPrompt || undefined,
  ].filter((line): line is string => Boolean(line));
  const pinnedMemoryLines = [
    "# Pinned Memory",
    "",
    "## Agent Memory",
    `- Agent ID: ${params.agentId}`,
    params.name ? `- Name: ${params.name}` : undefined,
    params.description ? `- Stable role: ${truncateText(params.description, 320)}` : undefined,
    params.skillSnapshotId ? `- Skill Snapshot: ${params.skillSnapshotId}` : undefined,
    params.systemPrompt
      ? `- Stable operating guidance: ${truncateText(params.systemPrompt, 1_000)}`
      : undefined,
  ].filter((line): line is string => Boolean(line));
  return [
    { name: DEFAULT_IDENTITY_FILENAME, content: identityLines.join("\n") },
    { name: DEFAULT_AGENTS_FILENAME, content: promptLines.join("\n") },
    { name: DEFAULT_PINNED_MEMORY_FILENAME, content: pinnedMemoryLines.join("\n") },
    ...buildPlatformDefaultSkillFiles(),
  ];
}

async function readTextFileIfExists(targetPath: string): Promise<string | undefined> {
  try {
    return await fs.readFile(targetPath, "utf-8");
  } catch {
    return undefined;
  }
}

function extractWorkspaceField(content: string | undefined, label: string): string | undefined {
  if (!content) {
    return undefined;
  }
  const match = content.match(new RegExp(`^- ${label}:\\s*(.+)$`, "m"));
  return match?.[1]?.trim() || undefined;
}

function extractWorkspaceSection(content: string | undefined, heading: string): string | undefined {
  if (!content) {
    return undefined;
  }
  const marker = `${heading}\n`;
  const startIndex = content.indexOf(marker);
  if (startIndex < 0) {
    return undefined;
  }
  const section = content.slice(startIndex + marker.length).trim();
  return section || undefined;
}

async function loadExistingWorkspaceMetadata(workspaceDir: string): Promise<{
  name?: string;
  description?: string;
  systemPrompt?: string;
  skillSnapshotId?: string;
}> {
  const [identityContent, agentsContent] = await Promise.all([
    readTextFileIfExists(path.join(workspaceDir, DEFAULT_IDENTITY_FILENAME)),
    readTextFileIfExists(path.join(workspaceDir, DEFAULT_AGENTS_FILENAME)),
  ]);
  return {
    name:
      extractWorkspaceField(agentsContent, "Name") ??
      extractWorkspaceField(identityContent, "Name"),
    description: extractWorkspaceField(agentsContent, "Description"),
    systemPrompt: extractWorkspaceSection(agentsContent, "## System Prompt"),
    skillSnapshotId: extractWorkspaceField(agentsContent, "Skill Snapshot"),
  };
}

async function collectMemoryMarkdownFiles(
  rootDir: string,
  relativeDir: string,
): Promise<Array<{ name: string; content: string }>> {
  const absDir = path.join(rootDir, relativeDir);
  if (!(await pathExists(absDir))) {
    return [];
  }
  const entries = await fs.readdir(absDir, { withFileTypes: true });
  const files: Array<{ name: string; content: string }> = [];
  for (const entry of entries) {
    const relativePath = path.posix.join(relativeDir, entry.name);
    const normalized = normalizeWorkspaceRelativePath(relativePath);
    if (!normalized) {
      continue;
    }
    if (entry.isDirectory()) {
      files.push(...(await collectMemoryMarkdownFiles(rootDir, normalized)));
      continue;
    }
    if (!entry.isFile() || !normalized.endsWith(".md")) {
      continue;
    }
    const content = await readTextFileIfExists(path.join(rootDir, normalized));
    if (typeof content === "string") {
      files.push({ name: normalized, content });
    }
  }
  return files;
}

async function listExportableReleaseFiles(
  workspaceDir: string,
): Promise<Array<{ name: string; content: string }>> {
  const files: Array<{ name: string; content: string }> = [];
  for (const relativePath of RELEASE_EXPORT_ROOT_FILES) {
    const content = await readTextFileIfExists(path.join(workspaceDir, relativePath));
    if (typeof content === "string") {
      files.push({ name: relativePath, content });
    }
  }
  files.push(...(await collectMemoryMarkdownFiles(workspaceDir, "memory")));
  return files.toSorted((a, b) => a.name.localeCompare(b.name));
}

async function ensureLocalAgentProvisioned(body: JsonObject): Promise<{
  cfg: ReturnType<typeof loadConfig>;
  agentId: string;
  localAgentKey: string;
  workspaceKey: string;
}> {
  const requestedAgentId = normalizeAgentId(
    readOptionalString(body, "agentId", "localAgentKey", "localAgentId") || "",
  );
  if (!requestedAgentId) {
    throw new Error("missing or invalid agentId");
  }

  const currentCfg = loadConfig();
  const workspaceDir = resolveAgentWorkspaceDir(currentCfg, requestedAgentId);
  const agentDir = resolveAgentDir(currentCfg, requestedAgentId);
  const preserveWorkspace =
    readOptionalBoolean(body, "preserveWorkspace", "preserveExistingWorkspace") === true;
  const createFreshWorkspace =
    readOptionalBoolean(body, "createFreshWorkspace", "freshWorkspace") === true;
  const replaceExistingWorkspace =
    readOptionalBoolean(
      body,
      "replaceExistingWorkspace",
      "replaceWorkspace",
      "atomicReplaceWorkspace",
    ) === true;
  const cloneFromLocalAgentKey = normalizeAgentId(
    readOptionalString(body, "cloneFromLocalAgentKey", "cloneFromAgentId") || "",
  );
  const existingEntries = [...listAgentEntries(currentCfg)];
  const existingIndex = existingEntries.findIndex(
    (entry) => normalizeAgentId(entry.id) === requestedAgentId,
  );
  const existingEntry = existingIndex >= 0 ? existingEntries[existingIndex] : undefined;
  const nextEntry = {
    ...existingEntry,
    id: requestedAgentId,
    name: readOptionalString(body, "name") ?? existingEntry?.name ?? requestedAgentId,
    workspace: workspaceDir,
    agentDir,
  };
  if (existingIndex >= 0) {
    existingEntries.splice(existingIndex, 1, nextEntry);
  } else {
    existingEntries.push(nextEntry);
  }

  await writeConfigFile({
    ...currentCfg,
    agents: {
      ...currentCfg.agents,
      list: existingEntries,
    },
  });

  const cfg = loadConfig();
  const sourceAgentId = resolveDefaultAgentId(cfg);
  const sourceAgentDir = resolveAgentDir(cfg, sourceAgentId);
  const targetAgentDir = resolveAgentDir(cfg, requestedAgentId);
  await fs.mkdir(targetAgentDir, { recursive: true, mode: 0o700 });
  if (sourceAgentId !== requestedAgentId) {
    await copyFileIfMissing(
      path.join(sourceAgentDir, "auth-profiles.json"),
      path.join(targetAgentDir, "auth-profiles.json"),
    );
    await copyFileIfMissing(
      path.join(sourceAgentDir, "models.json"),
      path.join(targetAgentDir, "models.json"),
    );
  }

  if (createFreshWorkspace) {
    // 历史语义是"擦掉 target 后再 clone / 留空目录"，会把训练人员在训练 workspace 里
    // 装好的 skills/<key>/、本地脚本、共享文件等一起删掉。新语义改为非破坏性：
    //   - 如果 target workspace 还不存在，且给了 cloneFromLocalAgentKey，则把 clone source
    //     的内容 cp 进去作为初始内容（v2 候选首次同步从 latest released clone 的诉求）。
    //   - 如果 target workspace 已经存在（用户在训练里已经迭代过一轮），保持原样不动，
    //     只让后续的 explicit workspaceFiles 覆盖控制面文档；防止重复点"同步训练状态"
    //     把训练里的 skill 等文件擦掉。
    if (cloneFromLocalAgentKey && cloneFromLocalAgentKey !== requestedAgentId) {
      const targetWorkspaceDir = resolveAgentWorkspaceDir(cfg, requestedAgentId);
      if (!(await pathExists(targetWorkspaceDir))) {
        await mergeWorkspaceTreeIfExists({
          cfg,
          sourceAgentId: cloneFromLocalAgentKey,
          targetWorkspaceDir,
        });
      }
    }
  }

  const explicitWorkspaceFiles = parseWorkspaceFilesFromValue(body.workspaceFiles);
  const hasExplicitWorkspaceFiles = explicitWorkspaceFiles.length > 0;
  const explicitWorkspaceIncludesBootstrap = workspaceFilesInclude(
    explicitWorkspaceFiles,
    DEFAULT_BOOTSTRAP_FILENAME,
  );
  // 兜底：只要请求里给了 mergeFromLocalAgentKey（=训练 workspace 的 localAgentKey），就走
  // mergeWorkspaceWithFiles 路径。即使 explicitWorkspaceFiles 为空（控制面没有内联 markdown），
  // 我们也要确保从训练 workspace 把 skills/<key>/、用户脚本等增量复制到 target；否则会落到
  // 下方的 "ensureAgentWorkspace + writeTextFile" 路径，那条路径完全不会去碰训练 workspace，
  // 就是导致"训练 workspace 加 skill → 发布上线后 serving workspace 没有"的根因之一。
  const rawMergeFromLocalAgentKey = readOptionalString(
    body,
    "mergeFromLocalAgentKey",
    "mergeFromAgentId",
    "sourceTrainingLocalAgentKey",
    "sourceLocalAgentKey",
  );
  const hasMergeFromLocalAgentKey =
    typeof rawMergeFromLocalAgentKey === "string" &&
    rawMergeFromLocalAgentKey.trim() !== "" &&
    normalizeAgentId(rawMergeFromLocalAgentKey) !== normalizeAgentId(requestedAgentId);
  const shouldMergeWorkspace =
    !preserveWorkspace &&
    (replaceExistingWorkspace || hasMergeFromLocalAgentKey) &&
    (explicitWorkspaceFiles.length > 0 || hasMergeFromLocalAgentKey);
  if (shouldMergeWorkspace) {
    // 历史语义是"用 explicit workspaceFiles 整个替换 target 目录（只有 .tri-layer-memory 幸存）"，
    // 这会把发布前用户在 target workspace 里安装的 skills/、iqiyi_source/、其他运行时文件全部清掉。
    // 新语义：把 mergeFromLocalAgentKey 指向的训练 workspace 增量合并进 target，再用 explicit
    // workspaceFiles 覆盖一遍控制面 markdown，target 里的"target 独有文件"被原样保留。
    logInfo(
      `control-plane: ensureLocalAgentProvisioned for "${requestedAgentId}" — taking workspace merge path (replaceExistingWorkspace=${replaceExistingWorkspace}, hasMergeFromLocalAgentKey=${hasMergeFromLocalAgentKey}, explicitWorkspaceFiles=${explicitWorkspaceFiles.length}).`,
    );
    await mergeWorkspaceWithFiles({
      workspaceDir: resolveAgentWorkspaceDir(cfg, requestedAgentId),
      files: explicitWorkspaceFiles,
      cfg,
      agentId: requestedAgentId,
      body,
    });
    return {
      cfg,
      agentId: requestedAgentId,
      localAgentKey: requestedAgentId,
      workspaceKey:
        path.basename(resolveAgentWorkspaceDir(cfg, requestedAgentId)) ||
        `workspace-${requestedAgentId}`,
    };
  }

  const workspace = await ensureAgentWorkspace({
    dir: resolveAgentWorkspaceDir(cfg, requestedAgentId),
    // When the control plane supplies an explicit workspace file list, the
    // workspace is managed by that list. Seeding OpenClaw's defaults here would
    // re-create BOOTSTRAP.md and conflict with plugin-driven memory flows.
    ensureBootstrapFiles: !hasExplicitWorkspaceFiles && !cfg.agents?.defaults?.skipBootstrap,
  });
  if (hasExplicitWorkspaceFiles && !explicitWorkspaceIncludesBootstrap) {
    await fs.rm(path.join(workspace.dir, DEFAULT_BOOTSTRAP_FILENAME), { force: true });
  }
  if (!preserveWorkspace) {
    const existingWorkspaceMetadata =
      explicitWorkspaceFiles.length > 0 ? {} : await loadExistingWorkspaceMetadata(workspace.dir);
    const workspaceFiles =
      explicitWorkspaceFiles.length > 0
        ? explicitWorkspaceFiles
        : buildDefaultWorkspaceFiles({
            agentId: requestedAgentId,
            name:
              readOptionalString(body, "name") ??
              existingEntry?.name ??
              existingWorkspaceMetadata.name,
            description:
              readOptionalString(body, "description") ?? existingWorkspaceMetadata.description,
            systemPrompt:
              readOptionalString(body, "systemPrompt") ?? existingWorkspaceMetadata.systemPrompt,
            skillSnapshotId:
              readOptionalString(body, "skillSnapshotId", "snapshotId") ??
              existingWorkspaceMetadata.skillSnapshotId,
          });
    for (const file of workspaceFiles) {
      await writeTextFile(path.join(workspace.dir, file.name), file.content);
    }
  }

  return {
    cfg,
    agentId: requestedAgentId,
    localAgentKey: requestedAgentId,
    workspaceKey: path.basename(workspace.dir) || `workspace-${requestedAgentId}`,
  };
}

function mergeSyncedRuntimeAgents(params: {
  currentAgents: ControlPlaneRuntimeAgent[];
  nextEntry: ControlPlaneRuntimeAgent;
}): ControlPlaneRuntimeAgent[] {
  const nextAgentId = normalizeAgentId(params.nextEntry.agentId);
  const nextRemoteAgentId = normalizeRemoteAgentId(params.nextEntry.remoteAgentId);
  const nextRole = params.nextEntry.runtimeRole;

  const occupiesSameSlot = (entry: ControlPlaneRuntimeAgent): boolean => {
    if (normalizeRemoteAgentId(entry.remoteAgentId) !== nextRemoteAgentId) {
      return false;
    }
    if (nextRole && entry.runtimeRole) {
      return entry.runtimeRole === nextRole;
    }
    return normalizeAgentId(entry.agentId) === nextAgentId;
  };

  return [...params.currentAgents.filter((entry) => !occupiesSameSlot(entry)), params.nextEntry];
}

function resolvePrimaryRuntimeRemoteAgentId(params: {
  cfg: ReturnType<typeof loadConfig>;
  currentState: ReturnType<typeof loadControlPlaneRuntimeState>;
  agents: ControlPlaneRuntimeAgent[];
  fallbackRemoteAgentId: string;
}): string {
  const defaultAgentId = resolveDefaultAgentId(params.cfg);
  const defaultAgentEntry = params.agents.find(
    (entry) => normalizeAgentId(entry.agentId) === defaultAgentId && entry.remoteAgentId,
  );
  if (defaultAgentEntry?.remoteAgentId) {
    return defaultAgentEntry.remoteAgentId;
  }
  const currentRemoteAgentId = normalizeRemoteAgentId(params.currentState.remoteAgentId);
  const currentEntry = params.agents.find(
    (entry) => normalizeRemoteAgentId(entry.remoteAgentId) === currentRemoteAgentId,
  );
  if (currentEntry?.remoteAgentId) {
    return currentEntry.remoteAgentId;
  }
  return params.fallbackRemoteAgentId;
}

async function upsertRuntimeAgent(params: {
  body: JsonObject;
  deploymentSource: "sync" | "release";
  defaultRuntimeRole?: ControlPlaneRuntimeRole;
}): Promise<{
  agentId: string;
  localAgentKey: string;
  workspaceKey: string;
  remoteAgentId: string;
  runtimeRole?: ControlPlaneRuntimeRole;
  sessionViews: ControlPlaneConversationView[];
  agentVersionId?: string;
  skillSnapshotId?: string;
  releaseId?: string;
  releaseVersion?: string;
  releaseStatus?: string;
  releaseManifest?: JsonObject;
  releaseFileCount?: number;
  totalAgents: number;
}> {
  const release = parseReleaseDescriptor(params.body);
  const provisionBody =
    release.releaseFiles.length > 0
      ? {
          ...params.body,
          workspaceFiles: release.releaseFiles,
          ...(params.deploymentSource === "release" &&
          readOptionalBoolean(params.body, "preserveWorkspace", "preserveExistingWorkspace") !==
            true
            ? { replaceExistingWorkspace: true }
            : {}),
        }
      : params.body;
  const provisioned = await ensureLocalAgentProvisioned(provisionBody);
  const agentId = provisioned.agentId;
  const remoteAgentId =
    typeof params.body.remoteAgentId === "string" && params.body.remoteAgentId.trim()
      ? params.body.remoteAgentId.trim()
      : `remote-${agentId || "agent"}`;
  const current = loadControlPlaneRuntimeState();
  const existingEntry = (current.agents ?? []).find(
    (item) => normalizeAgentId(item.agentId) === agentId,
  );
  const runtimeRole =
    normalizeRuntimeRole(
      readOptionalString(params.body, "runtimeRole", "targetRuntimeRole") ??
        readOptionalString(current as JsonObject, "runtimeRole"),
    ) ??
    params.defaultRuntimeRole ??
    existingEntry?.runtimeRole;
  const sessionViews = buildSessionViews(runtimeRole);
  const now = new Date().toISOString();
  const releaseStage = readOptionalString(params.body, "releaseStage");
  const releaseStatus =
    release.releaseStatus ??
    (releaseStage === "released" || releaseStage === "published" ? "released" : undefined) ??
    (params.deploymentSource === "release"
      ? "deployed"
      : runtimeRole === "training"
        ? (existingEntry?.releaseStatus ?? "draft")
        : existingEntry?.releaseStatus);
  const nextEntry: ControlPlaneRuntimeAgent = {
    ...existingEntry,
    agentId,
    name: readOptionalString(params.body, "name") ?? existingEntry?.name,
    remoteAgentId,
    localAgentKey: provisioned.localAgentKey,
    workspaceKey: provisioned.workspaceKey,
    agentVersionId:
      readOptionalString(params.body, "agentVersionId") ?? existingEntry?.agentVersionId,
    skillSnapshotId:
      readOptionalString(params.body, "skillSnapshotId", "snapshotId") ??
      existingEntry?.skillSnapshotId,
    runtimeRole,
    sessionViews,
    deploymentSource: params.deploymentSource,
    releaseId: release.releaseId ?? existingEntry?.releaseId,
    releaseVersion: release.releaseVersion ?? existingEntry?.releaseVersion,
    releaseStatus,
    releaseManifest: release.releaseManifest ?? existingEntry?.releaseManifest,
    releaseFileCount:
      release.releaseFiles.length > 0
        ? release.releaseFiles.length
        : existingEntry?.releaseFileCount,
    deployedAt: params.deploymentSource === "release" ? now : existingEntry?.deployedAt,
    status: "ready",
    updatedAt: now,
  };
  const agents = mergeSyncedRuntimeAgents({
    currentAgents: [...(current.agents ?? [])],
    nextEntry,
  });
  const state = mergeControlPlaneRuntimeState({
    runtimeRole: current.runtimeRole,
    sessionViews: buildSessionViews(current.runtimeRole),
    remoteAgentId: resolvePrimaryRuntimeRemoteAgentId({
      cfg: provisioned.cfg,
      currentState: current,
      agents,
      fallbackRemoteAgentId: remoteAgentId,
    }),
    agentVersion: readOptionalString(params.body, "agentVersionId") ?? current.agentVersion,
    skillSnapshotId:
      readOptionalString(params.body, "skillSnapshotId", "snapshotId") ?? current.skillSnapshotId,
    agents,
  });
  return {
    agentId,
    localAgentKey: provisioned.localAgentKey,
    workspaceKey: provisioned.workspaceKey,
    remoteAgentId,
    runtimeRole,
    sessionViews,
    agentVersionId: nextEntry.agentVersionId,
    skillSnapshotId: nextEntry.skillSnapshotId,
    releaseId: nextEntry.releaseId,
    releaseVersion: nextEntry.releaseVersion,
    releaseStatus: nextEntry.releaseStatus,
    releaseManifest: nextEntry.releaseManifest,
    releaseFileCount: nextEntry.releaseFileCount,
    totalAgents: state.agents?.length ?? 0,
  };
}

async function clearTrainingWorkspaceFromBody(params: {
  body: JsonObject;
  deployedAgentId: string;
}): Promise<{ agentId: string; workspaceKey: string } | undefined> {
  const clearTrainingRemoteAgentId = readOptionalString(
    params.body,
    "clearTrainingRemoteAgentId",
    "trainingRemoteAgentId",
  );
  const state = clearTrainingRemoteAgentId ? loadControlPlaneRuntimeState() : undefined;
  const trainingAgentFromRemote = state?.agents?.find(
    (entry) =>
      normalizeRemoteAgentId(entry.remoteAgentId) ===
        normalizeRemoteAgentId(clearTrainingRemoteAgentId) && entry.runtimeRole === "training",
  );
  const clearTrainingAgentId = normalizeAgentId(
    readOptionalString(
      params.body,
      "clearTrainingWorkspaceAgentId",
      "clearTrainingLocalAgentKey",
    ) ??
      trainingAgentFromRemote?.agentId ??
      "",
  );
  if (!clearTrainingAgentId || clearTrainingAgentId === normalizeAgentId(params.deployedAgentId)) {
    return undefined;
  }
  return clearLocalAgentWorkspace(loadConfig(), clearTrainingAgentId);
}

function buildReleaseExportPayload(params: {
  runtimeAgent: ControlPlaneRuntimeAgent;
  files: Array<{ name: string; content: string }>;
  release: ReleaseDescriptor;
  exportedAt: string;
}) {
  return {
    ok: true,
    remoteAgentId: params.runtimeAgent.remoteAgentId,
    agentId: params.runtimeAgent.agentId,
    runtimeRole: params.runtimeAgent.runtimeRole,
    release: {
      releaseId: params.release.releaseId ?? params.runtimeAgent.releaseId,
      releaseVersion:
        params.release.releaseVersion ??
        params.runtimeAgent.releaseVersion ??
        params.runtimeAgent.agentVersionId,
      releaseStatus:
        params.release.releaseStatus ?? params.runtimeAgent.releaseStatus ?? "released",
      exportedAt: params.exportedAt,
      agentVersionId: params.runtimeAgent.agentVersionId,
      skillSnapshotId: params.runtimeAgent.skillSnapshotId,
      manifest: params.release.releaseManifest ?? params.runtimeAgent.releaseManifest,
      files: params.files,
      fileCount: params.files.length,
    },
  };
}

async function deleteRuntimeAgentByRemoteAgentId(remoteAgentId: string): Promise<{
  status: number;
  body: JsonObject;
}> {
  const cfg = loadConfig();
  const current = loadControlPlaneRuntimeState();
  const runtimeAgent = (current.agents ?? []).find(
    (entry) =>
      normalizeRemoteAgentId(entry.remoteAgentId) === normalizeRemoteAgentId(remoteAgentId),
  );
  if (!runtimeAgent) {
    return {
      status: 404,
      body: {
        error: "remote agent is not synced to a local OpenClaw agent",
        remoteAgentId,
      },
    };
  }
  if (normalizeAgentId(runtimeAgent.agentId) === resolveDefaultAgentId(cfg)) {
    return {
      status: 409,
      body: {
        error: "refusing to delete the default runtime template agent",
        remoteAgentId,
        agentId: runtimeAgent.agentId,
      },
    };
  }

  const remainingConfigAgents = listAgentEntries(cfg).filter(
    (entry) => normalizeAgentId(entry.id) !== normalizeAgentId(runtimeAgent.agentId),
  );
  await writeConfigFile({
    ...cfg,
    agents: {
      ...cfg.agents,
      list: remainingConfigAgents,
    },
  });
  const nextCfg = loadConfig();
  await fs.rm(resolveAgentDir(nextCfg, runtimeAgent.agentId), {
    recursive: true,
    force: true,
  });
  await fs.rm(resolveAgentWorkspaceDir(nextCfg, runtimeAgent.agentId), {
    recursive: true,
    force: true,
  });
  const agents = (current.agents ?? []).filter(
    (entry) =>
      normalizeRemoteAgentId(entry.remoteAgentId) !== normalizeRemoteAgentId(remoteAgentId),
  );
  mergeControlPlaneRuntimeState({
    agents,
    remoteAgentId:
      agents.length > 0
        ? resolvePrimaryRuntimeRemoteAgentId({
            cfg: nextCfg,
            currentState: current,
            agents,
            fallbackRemoteAgentId: agents[0]?.remoteAgentId ?? "",
          })
        : undefined,
  });
  return {
    status: 200,
    body: {
      ok: true,
      remoteAgentId,
      agentId: runtimeAgent.agentId,
      localAgentKey: runtimeAgent.localAgentKey ?? runtimeAgent.agentId,
      workspaceKey: runtimeAgent.workspaceKey ?? null,
      status: "deleted",
      remainingAgents: agents.length,
    },
  };
}

function listPendingExecApprovalRecords(
  manager: NonNullable<ReturnType<typeof getGlobalExecApprovalManager>>,
): ExecApprovalRecord[] {
  const pending = (manager as unknown as { pending?: Map<string, { record: ExecApprovalRecord }> })
    .pending;
  if (!(pending instanceof Map)) {
    return [];
  }
  const records: ExecApprovalRecord[] = [];
  for (const entry of pending.values()) {
    if (entry.record.resolvedAtMs === undefined) {
      records.push(entry.record);
    }
  }
  return records;
}

export async function handleControlPlaneHttpRequest(
  req: IncomingMessage,
  res: ServerResponse,
): Promise<boolean> {
  const url = new URL(req.url ?? "/", "http://localhost");
  if (!url.pathname.startsWith(PREFIX)) {
    return false;
  }
  if (!authorizeBridge(req)) {
    sendJson(res, 401, { error: "unauthorized bridge token" });
    return true;
  }
  ensurePortalDeliverablesCleanupTimerStarted();

  if (url.pathname === `${PREFIX}/runtime-context`) {
    if (!ensureMethod(req, res, "GET")) {
      return true;
    }
    sendJson(res, 200, loadControlPlaneRuntimeState());
    return true;
  }

  if (url.pathname === `${PREFIX}/bootstrap`) {
    if (!ensureMethod(req, res, "POST")) {
      return true;
    }
    const body = await readBody(req);
    const current = loadControlPlaneRuntimeState();
    const runtimeRole =
      normalizeRuntimeRole(readOptionalString(body, "runtimeRole", "instanceRole")) ??
      current.runtimeRole;
    const state = mergeControlPlaneRuntimeState({
      workgroupId: readOptionalString(body, "workgroupId"),
      workgroupName: readOptionalString(body, "workgroupName"),
      instanceKey: readOptionalString(body, "instanceKey"),
      machineName: readOptionalString(body, "machineName"),
      runtimeRole,
      sessionViews: buildSessionViews(runtimeRole),
      bundleDir: readOptionalString(body, "bundleDir"),
      manifestPath: readOptionalString(body, "manifestPath"),
      skillSnapshotId: readOptionalString(body, "snapshotId", "skillSnapshotId"),
      traceContext: readOptionalString(body, "traceId", "traceContext"),
      instanceId:
        typeof body.instanceId === "string"
          ? body.instanceId
          : typeof body.instanceKey === "string"
            ? body.instanceKey
            : undefined,
    });
    sendJson(res, 200, { ok: true, state });
    return true;
  }

  if (url.pathname === `${PREFIX}/skills/snapshot/apply`) {
    if (!ensureMethod(req, res, "POST")) {
      return true;
    }
    const body = await readBody(req);
    const snapshotId = readOptionalString(body, "snapshotId", "skillSnapshotId") ?? "";
    const packages = Array.isArray(body.packages)
      ? body.packages
          .filter(
            (item): item is JsonObject =>
              Boolean(item) && typeof item === "object" && !Array.isArray(item),
          )
          .map((item) => ({
            skillKey: typeof item.skillKey === "string" ? item.skillKey : "",
            type: typeof item.type === "string" ? item.type : undefined,
            status: typeof item.status === "string" ? item.status : undefined,
            remoteSkillKey:
              typeof item.remoteSkillKey === "string" ? item.remoteSkillKey : undefined,
          }))
          .filter((item) => item.skillKey)
      : [];
    const current = loadControlPlaneRuntimeState();
    const state = mergeControlPlaneRuntimeState({
      skillSnapshotId: snapshotId || current.skillSnapshotId,
      skillSnapshot: snapshotId
        ? {
            snapshotId,
            appliedAt: new Date().toISOString(),
            packages,
          }
        : current.skillSnapshot,
    });
    sendJson(res, 200, {
      ok: true,
      snapshotId: state.skillSnapshotId,
      packagesApplied: packages.length,
    });
    return true;
  }

  if (url.pathname === `${PREFIX}/skills/registry/install`) {
    if (!ensureMethod(req, res, "POST")) {
      return true;
    }
    const body = await readBody(req);
    const agentId = normalizeAgentId(
      readOptionalString(body, "agentId", "localAgentKey", "localAgentId") ?? "",
    );
    if (!agentId) {
      sendJson(res, 400, { error: "missing or invalid agentId" });
      return true;
    }
    const dataObj = readOptionalObject(body, "data");
    const archiveBase64 =
      readOptionalString(body, "archiveBase64", "artifactBase64") ??
      (dataObj ? readOptionalString(dataObj, "archiveBase64", "artifactBase64") : undefined);
    const downloadUrl =
      readOptionalString(body, "downloadUrl") ??
      (dataObj ? readOptionalString(dataObj, "downloadUrl") : undefined);
    if (!archiveBase64 && !downloadUrl) {
      sendJson(res, 400, { error: "missing archiveBase64 or downloadUrl" });
      return true;
    }
    const skillKey =
      readOptionalString(body, "skillKey") ??
      (dataObj ? readOptionalString(dataObj, "skillKey") : undefined);
    if (!skillKey) {
      sendJson(res, 400, { error: "missing skillKey" });
      return true;
    }
    const cfg = loadConfig();
    const workspaceDir = resolveAgentWorkspaceDir(cfg, agentId);
    const artifactObj =
      readOptionalObject(body, "artifact") ??
      (dataObj ? readOptionalObject(dataObj, "artifact") : undefined);
    const artifactFormat =
      readOptionalString(body, "artifactFormat", "format") ??
      (artifactObj ? readOptionalString(artifactObj, "format") : undefined);
    const archiveFileName =
      readOptionalString(body, "archiveFileName", "fileName") ??
      (artifactObj ? readOptionalString(artifactObj, "fileName", "name") : undefined) ??
      (dataObj ? readOptionalString(dataObj, "archiveFileName", "fileName") : undefined);
    const expectedSha256 =
      readOptionalString(body, "sha256", "expectedSha256", "artifactSha256") ??
      (artifactObj ? readOptionalString(artifactObj, "sha256") : undefined);
    const stripRaw = body.stripComponents;
    const stripComponents =
      typeof stripRaw === "number" && Number.isFinite(stripRaw) ? stripRaw : undefined;
    const timeoutRaw = body.timeoutMs;
    const timeoutMs =
      typeof timeoutRaw === "number" && Number.isFinite(timeoutRaw) ? timeoutRaw : undefined;

    const result = archiveBase64
      ? await installSkillPackageFromInlineArchive({
          workspaceDir,
          archiveBase64,
          archiveFileName,
          skillKey,
          artifactFormat,
          expectedSha256,
          stripComponents,
          timeoutMs,
        })
      : await installSkillPackageFromRegistryDownload({
          workspaceDir,
          downloadUrl: downloadUrl!,
          skillKey,
          artifactFormat,
          expectedSha256,
          stripComponents,
          timeoutMs,
        });
    if (!result.ok) {
      sendJson(res, 400, { error: result.message });
      return true;
    }
    bumpSkillsSnapshotVersion({
      workspaceDir,
      reason: "manual",
      changedPath: result.installedPath,
    });
    sendJson(res, 200, {
      ok: true,
      agentId,
      skillKey: result.skillKey,
      installedPath: result.installedPath,
      bytes: result.bytes,
      sha256: result.sha256,
    });
    return true;
  }

  if (url.pathname === `${PREFIX}/agents/sync`) {
    if (!ensureMethod(req, res, "POST")) {
      return true;
    }
    const body = await readBody(req);
    try {
      const synced = await upsertRuntimeAgent({
        body,
        deploymentSource: "sync",
      });
      const shouldClearTrainingWorkspace =
        synced.runtimeRole === "serving" &&
        (synced.releaseStatus === "released" ||
          readOptionalString(body, "releaseStage") === "released");
      const clearedTrainingWorkspace = shouldClearTrainingWorkspace
        ? await clearTrainingWorkspaceFromBody({
            body,
            deployedAgentId: synced.agentId,
          })
        : undefined;
      sendJson(res, 200, {
        ok: true,
        agentId: synced.agentId,
        localAgentKey: synced.localAgentKey,
        workspaceKey: synced.workspaceKey,
        remoteAgentId: synced.remoteAgentId,
        runtimeRole: synced.runtimeRole,
        sessionViews: synced.sessionViews,
        agentVersionId: synced.agentVersionId,
        skillSnapshotId: synced.skillSnapshotId,
        releaseId: synced.releaseId,
        releaseVersion: synced.releaseVersion,
        releaseStatus: synced.releaseStatus,
        releaseFileCount: synced.releaseFileCount,
        clearedTrainingWorkspace,
        status: "ready",
        totalAgents: synced.totalAgents,
      });
    } catch (error) {
      sendJson(res, 400, {
        error: error instanceof Error ? error.message : String(error),
      });
    }
    return true;
  }

  if (url.pathname === `${PREFIX}/agents/deploy`) {
    if (!ensureMethod(req, res, "POST")) {
      return true;
    }
    const body = await readBody(req);
    try {
      const deployed = await upsertRuntimeAgent({
        body,
        deploymentSource: "release",
        defaultRuntimeRole: "serving",
      });
      const clearedTrainingWorkspace = await clearTrainingWorkspaceFromBody({
        body,
        deployedAgentId: deployed.agentId,
      });
      sendJson(res, 200, {
        ok: true,
        agentId: deployed.agentId,
        localAgentKey: deployed.localAgentKey,
        workspaceKey: deployed.workspaceKey,
        remoteAgentId: deployed.remoteAgentId,
        runtimeRole: deployed.runtimeRole,
        sessionViews: deployed.sessionViews,
        agentVersionId: deployed.agentVersionId,
        skillSnapshotId: deployed.skillSnapshotId,
        releaseId: deployed.releaseId,
        releaseVersion: deployed.releaseVersion,
        releaseStatus: deployed.releaseStatus,
        releaseFileCount: deployed.releaseFileCount,
        clearedTrainingWorkspace,
        status: "deployed",
        totalAgents: deployed.totalAgents,
      });
    } catch (error) {
      sendJson(res, 400, {
        error: error instanceof Error ? error.message : String(error),
      });
    }
    return true;
  }

  // ==================== Workspace Archive Export ====================
  const workspaceArchiveMatch = url.pathname.match(
    new RegExp(
      `^${PREFIX.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}/agents/([^/]+)/workspace/archive$`,
    ),
  );
  if (workspaceArchiveMatch) {
    if (!ensureMethod(req, res, "GET")) {
      return true;
    }
    const remoteAgentId = workspaceArchiveMatch[1] ?? "";
    const current = loadControlPlaneRuntimeState();
    const runtimeAgent = (current.agents ?? []).find(
      (entry) =>
        normalizeRemoteAgentId(entry.remoteAgentId) === normalizeRemoteAgentId(remoteAgentId),
    );
    if (!runtimeAgent) {
      sendJson(res, 404, { error: "agent not found", remoteAgentId });
      return true;
    }
    try {
      const cfg = loadConfig();
      const workspaceDir = resolveAgentWorkspaceDir(cfg, runtimeAgent.agentId);
      try {
        await fs.access(workspaceDir);
      } catch {
        sendJson(res, 404, { error: "workspace directory does not exist", remoteAgentId });
        return true;
      }
      const archiveFiles: Array<{ relativePath: string; contentBase64: string }> = [];
      let fileCount = 0;
      let totalSize = 0;

      async function collectFiles(dir: string, prefix: string) {
        let entries: import("node:fs").Dirent[];
        try {
          entries = await fs.readdir(dir, { withFileTypes: true });
        } catch {
          return;
        }
        for (const entry of entries) {
          const fullPath = path.join(dir, entry.name);
          const relPath = prefix ? `${prefix}/${entry.name}` : entry.name;
          if (entry.name === ".git" || entry.name === "node_modules") {
            continue;
          }
          if (entry.isDirectory()) {
            await collectFiles(fullPath, relPath);
          } else if (entry.isFile()) {
            try {
              const content = await fs.readFile(fullPath);
              archiveFiles.push({
                relativePath: relPath,
                contentBase64: content.toString("base64"),
              });
              fileCount++;
              totalSize += content.length;
            } catch {
              // skip unreadable files
            }
          }
        }
      }

      await collectFiles(workspaceDir, "");
      sendJson(res, 200, {
        ok: true,
        agentId: runtimeAgent.agentId,
        remoteAgentId: runtimeAgent.remoteAgentId,
        workspaceDir,
        fileCount,
        archiveSizeBytes: totalSize,
        archiveBase64: Buffer.from(JSON.stringify(archiveFiles)).toString("base64"),
      });
    } catch (error) {
      sendJson(res, 500, { error: error instanceof Error ? error.message : String(error) });
    }
    return true;
  }

  // ==================== Workspace Archive Import ====================
  if (url.pathname === `${PREFIX}/agents/import-archive`) {
    if (!ensureMethod(req, res, "POST")) {
      return true;
    }
    const body = await readBody(req);
    try {
      const requestedAgentId = normalizeAgentId(
        readOptionalString(body, "agentId", "localAgentKey") || "",
      );
      if (!requestedAgentId) {
        sendJson(res, 400, { error: "agentId is required" });
        return true;
      }
      const archiveBase64 = readOptionalString(body, "archiveBase64") || "";
      if (!archiveBase64) {
        sendJson(res, 400, { error: "archiveBase64 is required" });
        return true;
      }

      const cfg = loadConfig();
      const workspaceDir = resolveAgentWorkspaceDir(cfg, requestedAgentId);
      const agentDir = resolveAgentDir(cfg, requestedAgentId);

      const agentsList = cfg.agents?.list ?? [];
      const existingEntry = agentsList.find(
        (entry) => normalizeAgentId(entry.id || "") === requestedAgentId,
      );
      if (!existingEntry) {
        const updatedList = [
          ...agentsList,
          { id: requestedAgentId, workspace: workspaceDir, agentDir },
        ];
        await writeConfigFile({ ...cfg, agents: { ...cfg.agents, list: updatedList } });
      }

      await fs.mkdir(workspaceDir, { recursive: true });

      let archiveFiles: Array<{ relativePath: string; contentBase64: string }>;
      try {
        archiveFiles = JSON.parse(Buffer.from(archiveBase64, "base64").toString("utf-8"));
      } catch {
        sendJson(res, 400, { error: "invalid archiveBase64 format" });
        return true;
      }

      let restoredCount = 0;
      for (const file of archiveFiles) {
        if (!file.relativePath || typeof file.contentBase64 !== "string") {
          continue;
        }
        const normalized = file.relativePath.replace(/\.\./g, "").replace(/^\//, "");
        if (!normalized) {
          continue;
        }
        const targetPath = path.join(workspaceDir, normalized);
        if (!isPathInside(targetPath, workspaceDir)) {
          continue;
        }
        await fs.mkdir(path.dirname(targetPath), { recursive: true });
        await fs.writeFile(targetPath, Buffer.from(file.contentBase64, "base64"));
        restoredCount++;
      }

      const reloadedCfg = loadConfig();
      await ensureAgentWorkspace({
        dir: resolveAgentWorkspaceDir(reloadedCfg, requestedAgentId),
        ensureBootstrapFiles: true,
      });

      const defaultAgentId = resolveDefaultAgentId(reloadedCfg);
      if (requestedAgentId !== defaultAgentId) {
        const defaultAgentDir = resolveAgentDir(reloadedCfg, defaultAgentId);
        for (const seedFile of ["auth-profiles.json", "models.json"]) {
          const src = path.join(defaultAgentDir, seedFile);
          const dst = path.join(agentDir, seedFile);
          try {
            await fs.access(dst);
          } catch {
            try {
              await fs.mkdir(path.dirname(dst), { recursive: true });
              await fs.copyFile(src, dst);
            } catch {
              // skip if source doesn't exist
            }
          }
        }
      }

      const remoteAgentId = requestedAgentId;
      const state = loadControlPlaneRuntimeState();
      const existingAgent = (state.agents ?? []).find(
        (a) => normalizeAgentId(a.agentId) === requestedAgentId,
      );
      if (!existingAgent) {
        const updatedAt = new Date().toISOString();
        state.agents = [
          ...(state.agents ?? []),
          {
            agentId: requestedAgentId,
            remoteAgentId,
            localAgentKey: requestedAgentId,
            workspaceKey: requestedAgentId,
            runtimeRole: "serving",
            status: "imported",
            updatedAt,
          } satisfies ControlPlaneRuntimeAgent,
        ];
        saveControlPlaneRuntimeState(state);
      }

      sendJson(res, 200, {
        ok: true,
        agentId: requestedAgentId,
        remoteAgentId,
        localAgentKey: requestedAgentId,
        workspaceKey: requestedAgentId,
        restoredFileCount: restoredCount,
      });
    } catch (error) {
      sendJson(res, 500, { error: error instanceof Error ? error.message : String(error) });
    }
    return true;
  }

  const releaseExportMatch = url.pathname.match(
    new RegExp(`^${PREFIX.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}/agents/([^/]+)/release/export$`),
  );
  if (releaseExportMatch) {
    if (!ensureMethod(req, res, "POST")) {
      return true;
    }
    const remoteAgentId = releaseExportMatch[1] ?? "";
    const body = await readBody(req);
    const current = loadControlPlaneRuntimeState();
    const runtimeAgent = (current.agents ?? []).find(
      (entry) =>
        normalizeRemoteAgentId(entry.remoteAgentId) === normalizeRemoteAgentId(remoteAgentId),
    );
    if (!runtimeAgent) {
      sendJson(res, 404, {
        error: "remote agent is not synced to a local OpenClaw agent",
        remoteAgentId,
      });
      return true;
    }
    const cfg = loadConfig();
    const workspaceDir = resolveAgentWorkspaceDir(cfg, runtimeAgent.agentId);
    const files = await listExportableReleaseFiles(workspaceDir);
    const release = parseReleaseDescriptor(body);
    const exportedAt = new Date().toISOString();
    const nextAgent: ControlPlaneRuntimeAgent = {
      ...runtimeAgent,
      releaseId:
        release.releaseId ??
        runtimeAgent.releaseId ??
        `rel-${normalizeAgentId(runtimeAgent.agentId)}-${Date.now().toString(36)}`,
      releaseVersion:
        release.releaseVersion ?? runtimeAgent.releaseVersion ?? runtimeAgent.agentVersionId,
      releaseStatus: release.releaseStatus ?? runtimeAgent.releaseStatus ?? "released",
      releaseManifest: release.releaseManifest ?? runtimeAgent.releaseManifest,
      releaseFileCount: files.length,
      exportedAt,
      updatedAt: exportedAt,
    };
    mergeControlPlaneRuntimeState({
      agents: (current.agents ?? []).map((entry) =>
        normalizeRemoteAgentId(entry.remoteAgentId) === normalizeRemoteAgentId(remoteAgentId)
          ? nextAgent
          : entry,
      ),
    });
    sendJson(
      res,
      200,
      buildReleaseExportPayload({
        runtimeAgent: nextAgent,
        files,
        release,
        exportedAt,
      }),
    );
    return true;
  }

  const undeployMatch = url.pathname.match(
    new RegExp(`^${PREFIX.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}/agents/([^/]+)/undeploy$`),
  );
  if (undeployMatch) {
    if (!ensureMethod(req, res, "POST")) {
      return true;
    }
    const remoteAgentId = undeployMatch[1] ?? "";
    const result = await deleteRuntimeAgentByRemoteAgentId(remoteAgentId);
    sendJson(res, result.status, {
      ...result.body,
      status: result.status === 200 ? "undeployed" : result.body.status,
    });
    return true;
  }

  const deleteAgentMatch = url.pathname.match(
    new RegExp(`^${PREFIX.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}/agents/([^/]+)$`),
  );
  if (deleteAgentMatch) {
    if (!ensureMethod(req, res, "DELETE")) {
      return true;
    }
    const remoteAgentId = deleteAgentMatch[1] ?? "";
    const result = await deleteRuntimeAgentByRemoteAgentId(remoteAgentId);
    sendJson(res, result.status, result.body);
    return true;
  }

  if (url.pathname === `${PREFIX}/agents/delete`) {
    if (!ensureMethod(req, res, "POST")) {
      return true;
    }
    const body = await readBody(req);
    const remoteAgentId =
      typeof body.remoteAgentId === "string" && body.remoteAgentId.trim()
        ? body.remoteAgentId.trim()
        : "";
    if (!remoteAgentId) {
      sendJson(res, 400, { error: "missing or invalid remoteAgentId" });
      return true;
    }
    const result = await deleteRuntimeAgentByRemoteAgentId(remoteAgentId);
    sendJson(res, result.status, result.body);
    return true;
  }

  const sharedFilesMatch = url.pathname.match(
    new RegExp(`^${PREFIX.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}/agents/([^/]+)/files$`),
  );
  if (sharedFilesMatch) {
    const remoteAgentId = sharedFilesMatch[1] ?? "";
    const resolvedTarget = resolvePortalTargetAgent(remoteAgentId);
    if (!resolvedTarget) {
      sendJson(res, 404, {
        error: "remote agent is not synced to a local OpenClaw agent",
        remoteAgentId,
      });
      return true;
    }
    const workspaceDir = resolveAgentWorkspaceDir(resolvedTarget.cfg, resolvedTarget.agentId);
    if (req.method === "GET") {
      const files = await listPortalSharedFiles(workspaceDir);
      sendJson(res, 200, {
        ok: true,
        remoteAgentId,
        agentId: resolvedTarget.agentId,
        sourceDir: PORTAL_SHARED_FILES_DIRNAME,
        files,
      });
      return true;
    }
    if (req.method === "POST") {
      const body = await readBody(req);
      const files = Array.isArray(body.files)
        ? body.files.filter(
            (item): item is JsonObject =>
              Boolean(item) && typeof item === "object" && !Array.isArray(item),
          )
        : [];
      if (files.length === 0) {
        sendJson(res, 400, { error: "missing files" });
        return true;
      }
      try {
        const actor = readOptionalObject(body, "actor");
        const uploaded = await upsertPortalSharedFiles({
          workspaceDir,
          files: files.map((item) => ({
            fileName: readOptionalString(item, "fileName"),
            relativePath: readOptionalString(item, "relativePath", "fileName"),
            mimeType: readOptionalString(item, "mimeType"),
            contentBase64: readOptionalString(item, "contentBase64"),
          })),
          overwrite: body.overwrite !== false,
          uploadedBy:
            (actor ? readOptionalString(actor, "username", "userId") : undefined) ??
            readOptionalString(body, "uploadedBy", "username", "userId") ??
            null,
        });
        sendJson(res, 200, {
          ok: true,
          remoteAgentId,
          agentId: resolvedTarget.agentId,
          sourceDir: PORTAL_SHARED_FILES_DIRNAME,
          files: uploaded,
        });
      } catch (error) {
        sendJson(res, 400, {
          error: error instanceof Error ? error.message : String(error),
          remoteAgentId,
          agentId: resolvedTarget.agentId,
        });
      }
      return true;
    }
    if (!ensureMethod(req, res, ["GET", "POST"])) {
      return true;
    }
  }

  const sharedFileDeleteMatch = url.pathname.match(
    new RegExp(`^${PREFIX.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}/agents/([^/]+)/files/(.+)$`),
  );
  if (sharedFileDeleteMatch) {
    if (!ensureMethod(req, res, "DELETE")) {
      return true;
    }
    const remoteAgentId = sharedFileDeleteMatch[1] ?? "";
    const fileId = decodeURIComponent(sharedFileDeleteMatch[2] ?? "");
    const resolvedTarget = resolvePortalTargetAgent(remoteAgentId);
    if (!resolvedTarget) {
      sendJson(res, 404, {
        error: "remote agent is not synced to a local OpenClaw agent",
        remoteAgentId,
      });
      return true;
    }
    try {
      const deleted = await deletePortalSharedFile(
        resolveAgentWorkspaceDir(resolvedTarget.cfg, resolvedTarget.agentId),
        fileId,
      );
      sendJson(res, 200, {
        ok: true,
        remoteAgentId,
        agentId: resolvedTarget.agentId,
        deleted,
      });
    } catch (error) {
      sendJson(res, 400, {
        error: error instanceof Error ? error.message : String(error),
        remoteAgentId,
        agentId: resolvedTarget.agentId,
      });
    }
    return true;
  }

  if (url.pathname === `${PREFIX}/portal/sessions`) {
    if (!ensureMethod(req, res, "POST")) {
      return true;
    }
    const body = await readBody(req);
    const remoteAgentId =
      typeof body.remoteAgentId === "string" && body.remoteAgentId.trim()
        ? body.remoteAgentId.trim()
        : undefined;
    if (!remoteAgentId) {
      sendJson(res, 400, { error: "missing or invalid remoteAgentId" });
      return true;
    }
    const mode = normalizePortalMode(body.mode);
    const conversationView = resolveConversationView(mode);
    const preferredLocalAgentKey = readOptionalString(body, "localAgentKey", "agentId");
    const resolvedTarget = resolvePortalTargetAgent(
      remoteAgentId,
      conversationView,
      preferredLocalAgentKey,
    );
    if (!resolvedTarget) {
      sendJson(res, 404, {
        error: preferredLocalAgentKey
          ? "remote agent is not synced to this localAgentKey on the runtime"
          : "remote agent is not synced to a local OpenClaw agent",
        remoteAgentId,
        ...(preferredLocalAgentKey ? { localAgentKey: preferredLocalAgentKey } : {}),
      });
      return true;
    }
    const runtimeRole =
      resolvedTarget.runtimeAgent?.runtimeRole ?? resolvedTarget.runtimeState.runtimeRole;
    const sessionViews =
      resolvedTarget.runtimeAgent?.sessionViews ?? buildSessionViews(runtimeRole);
    if (!sessionViews.includes(conversationView)) {
      sendJson(res, 409, {
        error: "requested conversation view is not supported on this runtime agent",
        code: "SESSION_VIEW_NOT_SUPPORTED",
        remoteAgentId,
        conversationView,
        runtimeRole,
        sessionViews,
      });
      return true;
    }
    const traceId = readOptionalString(body, "traceId");
    const portalSessionId = readOptionalString(body, "portalSessionId");
    const userContext = isJsonObject(body.userContext) ? body.userContext : undefined;
    const now = new Date().toISOString();
    const remoteSessionId = `rs_${randomUUID().replace(/-/g, "")}`;
    const sessionKey = buildPortalSessionKey({
      cfg: resolvedTarget.cfg,
      agentId: resolvedTarget.agentId,
      remoteSessionId,
      conversationView,
      revision: 0,
    });
    const writePolicy = buildPortalWritePolicy(conversationView);
    portalSessions.set(remoteSessionId, {
      remoteAgentId,
      agentId: resolvedTarget.agentId,
      sessionKey,
      sessionRevision: 0,
      turnCount: 0,
      portalSessionId,
      mode,
      conversationView,
      runtimeRole,
      sessionViews,
      writePolicy,
      traceId,
      userContext,
      createdAt: now,
      updatedAt: now,
      agentVersionId: resolvedTarget.runtimeAgent?.agentVersionId,
      skillSnapshotId: resolvedTarget.runtimeAgent?.skillSnapshotId,
      externalSkillLookupAllowed: false,
      releaseId: resolvedTarget.runtimeAgent?.releaseId,
      releaseVersion: resolvedTarget.runtimeAgent?.releaseVersion,
      releaseStatus: resolvedTarget.runtimeAgent?.releaseStatus,
    });
    const sessionCreatedEvent = buildPortalRuntimeEvent({
      eventType: "session.created",
      level: "info",
      message: "Portal session created on runtime",
      payload: {
        remoteSessionId,
        remoteAgentId,
        agentId: resolvedTarget.agentId,
        mode,
        conversationView,
        traceId: traceId ?? null,
        portalSessionId: portalSessionId ?? null,
      },
      createdAt: now,
    });
    sendJson(res, 200, {
      ok: true,
      remoteSessionId,
      remoteAgentId,
      agentId: resolvedTarget.agentId,
      localAgentKey: resolvedTarget.runtimeAgent?.localAgentKey ?? resolvedTarget.agentId,
      workspaceKey: resolvedTarget.runtimeAgent?.workspaceKey,
      mode,
      conversationView,
      runtimeRole,
      sessionViews,
      writePolicy,
      agentVersionId: resolvedTarget.runtimeAgent?.agentVersionId,
      releaseId: resolvedTarget.runtimeAgent?.releaseId,
      releaseVersion: resolvedTarget.runtimeAgent?.releaseVersion,
      releaseStatus: resolvedTarget.runtimeAgent?.releaseStatus,
      status: "ready",
      runtimeEvents: [sessionCreatedEvent],
    });
    return true;
  }

  const portalDeliverablesMatch = url.pathname.match(
    new RegExp(
      `^${PREFIX.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}/portal/sessions/([^/]+)/deliverables$`,
    ),
  );
  if (portalDeliverablesMatch) {
    if (!ensureMethod(req, res, "GET")) {
      return true;
    }
    const remoteSessionId = decodeURIComponent(portalDeliverablesMatch[1] ?? "");
    const session = portalSessions.get(remoteSessionId);
    if (!session) {
      sendJson(res, 404, { ok: false, error: "session not found", remoteSessionId });
      return true;
    }
    const cfg = loadConfig();
    const workspaceDir = resolveAgentWorkspaceDir(cfg, session.agentId);
    const portalSessionId = session.portalSessionId ?? remoteSessionId;
    const deliverables = await listPortalDeliverablesForSession(workspaceDir, portalSessionId);
    sendJson(res, 200, {
      ok: true,
      remoteSessionId,
      portalSessionId,
      deliverablesDir: `${PORTAL_DELIVERABLES_DIRNAME}/${normalizePortalDeliverablesSegment(portalSessionId)}`,
      deliverables,
    });
    return true;
  }

  const portalDeliverableContentMatch = url.pathname.match(
    new RegExp(
      `^${PREFIX.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}/portal/sessions/([^/]+)/deliverables/(.+)/(download|preview)$`,
    ),
  );
  if (portalDeliverableContentMatch) {
    if (!ensureMethod(req, res, "GET")) {
      return true;
    }
    const remoteSessionId = decodeURIComponent(portalDeliverableContentMatch[1] ?? "");
    const artifactId = decodeURIComponent(portalDeliverableContentMatch[2] ?? "");
    const action = portalDeliverableContentMatch[3] ?? "";
    const session = portalSessions.get(remoteSessionId);
    if (!session) {
      sendJson(res, 404, { ok: false, error: "session not found", remoteSessionId });
      return true;
    }
    const cfg = loadConfig();
    const workspaceDir = resolveAgentWorkspaceDir(cfg, session.agentId);
    const portalSessionId = session.portalSessionId ?? remoteSessionId;
    try {
      const deliverable = await readPortalDeliverable(workspaceDir, portalSessionId, artifactId);
      if (action === "preview") {
        if (deliverable.record.previewType !== "html") {
          sendJson(res, 400, {
            ok: false,
            error: "deliverable preview only supports html files",
            artifactId,
          });
          return true;
        }
        sendJson(res, 200, {
          ok: true,
          artifact: deliverable.record,
          html: deliverable.content.toString("utf-8"),
        });
        return true;
      }
      sendJson(res, 200, {
        ok: true,
        artifact: deliverable.record,
        fileName: deliverable.record.fileName,
        mimeType: deliverable.record.mimeType,
        contentBase64: deliverable.content.toString("base64"),
      });
    } catch (error) {
      sendJson(res, 404, {
        ok: false,
        error: error instanceof Error ? error.message : String(error),
        remoteSessionId,
        artifactId,
      });
    }
    return true;
  }

  const portalDeliverableDeleteMatch = url.pathname.match(
    new RegExp(
      `^${PREFIX.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}/portal/sessions/([^/]+)/deliverables/(.+)$`,
    ),
  );
  if (portalDeliverableDeleteMatch) {
    if (!ensureMethod(req, res, "DELETE")) {
      return true;
    }
    const remoteSessionId = decodeURIComponent(portalDeliverableDeleteMatch[1] ?? "");
    const artifactId = decodeURIComponent(portalDeliverableDeleteMatch[2] ?? "");
    const session = portalSessions.get(remoteSessionId);
    if (!session) {
      sendJson(res, 404, { ok: false, error: "session not found", remoteSessionId });
      return true;
    }
    const cfg = loadConfig();
    const workspaceDir = resolveAgentWorkspaceDir(cfg, session.agentId);
    const portalSessionId = session.portalSessionId ?? remoteSessionId;
    try {
      const deleted = await deletePortalDeliverable(workspaceDir, portalSessionId, artifactId);
      sendJson(res, 200, {
        ok: true,
        remoteSessionId,
        portalSessionId,
        deleted,
      });
    } catch (error) {
      sendJson(res, 400, {
        ok: false,
        error: error instanceof Error ? error.message : String(error),
        remoteSessionId,
        artifactId,
      });
    }
    return true;
  }

  const portalSessionStatusMatch = url.pathname.match(
    new RegExp(`^${PREFIX.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}/portal/sessions/([^/]+)$`),
  );
  if (portalSessionStatusMatch) {
    if (!ensureMethod(req, res, "GET")) {
      return true;
    }
    const remoteSessionId = portalSessionStatusMatch[1] ?? "";
    const session = portalSessions.get(remoteSessionId);
    if (!session) {
      sendJson(res, 404, { ok: false, error: "session not found", remoteSessionId });
      return true;
    }
    sendJson(res, 200, {
      ok: true,
      remoteSessionId,
      portalSessionId: session.portalSessionId ?? null,
      remoteAgentId: session.remoteAgentId,
      agentId: session.agentId,
      mode: session.mode,
      conversationView: session.conversationView,
      runtimeRole: session.runtimeRole,
      updatedAt: session.updatedAt,
      turnCount: session.turnCount,
    });
    return true;
  }

  const portalMessageAbortMatch = url.pathname.match(
    new RegExp(
      `^${PREFIX.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}/portal/sessions/([^/]+)/messages/abort$`,
    ),
  );
  if (portalMessageAbortMatch) {
    if (!ensureMethod(req, res, "POST")) {
      return true;
    }
    const remoteSessionId = portalMessageAbortMatch[1] ?? "";
    const body = await readBody(req);
    const session = portalSessions.get(remoteSessionId);
    const runId = readOptionalString(body, "runId") ?? session?.lastRunId ?? null;
    if (!session && !runId) {
      sendJson(res, 404, { ok: false, error: "session not found", remoteSessionId });
      return true;
    }
    if (!runId) {
      sendJson(res, 200, {
        ok: true,
        aborted: false,
        runId: null,
        remoteSessionId,
        portalSessionId: session?.portalSessionId ?? null,
      });
      return true;
    }
    const abortResult = abortTrackedPortalRun(runId);
    sendJson(res, 200, {
      ok: true,
      aborted: abortResult.aborted,
      runId,
      remoteSessionId: abortResult.remoteSessionId ?? remoteSessionId,
      portalSessionId: abortResult.portalSessionId ?? session?.portalSessionId ?? null,
      stoppedAt: abortResult.stoppedAt,
    });
    return true;
  }

  const portalRunAbortMatch = url.pathname.match(
    new RegExp(`^${PREFIX.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}/portal/runs/([^/]+)/abort$`),
  );
  if (portalRunAbortMatch) {
    if (!ensureMethod(req, res, "POST")) {
      return true;
    }
    const runId = portalRunAbortMatch[1] ?? "";
    const abortResult = abortTrackedPortalRun(runId);
    sendJson(res, 200, {
      ok: true,
      aborted: abortResult.aborted,
      runId,
      remoteSessionId: abortResult.remoteSessionId ?? null,
      portalSessionId: abortResult.portalSessionId ?? null,
      stoppedAt: abortResult.stoppedAt,
    });
    return true;
  }

  const portalMessagesMatch = url.pathname.match(
    new RegExp(
      `^${PREFIX.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}/portal/sessions/([^/]+)/messages$`,
    ),
  );
  if (portalMessagesMatch) {
    if (!ensureMethod(req, res, "POST")) {
      return true;
    }
    const remoteSessionId = portalMessagesMatch[1];
    const session = portalSessions.get(remoteSessionId);
    if (!session) {
      sendJson(res, 404, { error: "session not found", remoteSessionId });
      return true;
    }
    const body = await readBody(req);
    const message = typeof body.message === "string" ? body.message.trim() : "";
    if (!message) {
      sendJson(res, 400, { error: "missing or invalid message" });
      return true;
    }
    const attachments: ChatAttachment[] = Array.isArray(body.attachments) ? body.attachments : [];
    const traceId = readOptionalString(body, "traceId");
    const portalSessionId = readOptionalString(body, "portalSessionId");
    const memoryContext = readOptionalObject(body, "memoryContext") as
      | PortalMemoryContext
      | undefined;
    const memoryPolicy = readOptionalObject(body, "memoryPolicy") as PortalMemoryPolicy | undefined;
    const memoryContextPrompt = buildPortalMemoryContextPrompt(memoryContext);
    if (memoryContext || memoryPolicy) {
      logInfo(
        `control-plane: portal message received memory context (traceId=${traceId ?? session.traceId ?? "n/a"}, remoteSessionId=${remoteSessionId}, agentId=${session.agentId}, memoryContext=${safeJsonStringify(
          summarizePortalMemoryContextForLog(memoryContext),
        )}, memoryPolicy=${safeJsonStringify(summarizePortalMemoryPolicyForLog(memoryPolicy))})`,
      );
    }
    if (memoryContextPrompt) {
      logInfo(
        `control-plane: portal message injecting memory prompt (traceId=${traceId ?? session.traceId ?? "n/a"}, remoteSessionId=${remoteSessionId}, agentId=${session.agentId}, promptPreview=${safeJsonStringify(
          memoryContextPrompt.slice(0, 1200),
        )})`,
      );
    }
    const runId = `run_${randomUUID().replace(/-/g, "")}`;
    const runStartedAt = new Date().toISOString();
    const runStartedEvent = buildPortalRuntimeEvent({
      eventType: "run.started",
      level: "info",
      message: `Portal run ${runId} started`,
      payload: {
        runId,
        remoteSessionId,
        portalSessionId: portalSessionId ?? session.portalSessionId ?? null,
        traceId: traceId ?? session.traceId ?? null,
        agentId: session.agentId,
        mode: session.mode,
      },
      createdAt: runStartedAt,
    });
    let nextSession: PortalSessionRecord = {
      ...session,
      portalSessionId: portalSessionId ?? session.portalSessionId,
      traceId: traceId ?? session.traceId,
      updatedAt: new Date().toISOString(),
      lastRunId: runId,
    };
    if (nextSession.mode === "training") {
      if (looksLikeExternalSkillLookupApproval(message)) {
        nextSession = { ...nextSession, externalSkillLookupAllowed: true };
      } else if (looksLikeExternalSkillLookupRevocation(message)) {
        nextSession = { ...nextSession, externalSkillLookupAllowed: false };
      }
    }
    portalSessions.set(remoteSessionId, nextSession);
    const skillSearchPrefetch = await prefetchPortalSkillSearch({
      session: nextSession,
      message,
    });
    savePortalRun({
      runId,
      remoteSessionId,
      portalSessionId: portalSessionId ?? session.portalSessionId,
      traceId: traceId ?? session.traceId,
      status: "started",
      startedAt: runStartedAt,
      streamSeq: 0,
      timeline: [{ phase: "started", at: runStartedAt }],
    });

    let effectiveMessage = message;
    let parsedImages: ChatImageContent[] = [];
    let parsedImageOrder: PromptImageOrderEntry[] = [];

    if (attachments.length > 0) {
      const parsed = await parseMessageWithAttachments(message, attachments, {
        maxBytes: 5_000_000,
        supportsImages: true,
      });
      effectiveMessage = parsed.message;
      parsedImages = parsed.images;
      parsedImageOrder = parsed.imageOrder;
    }

    if (isSseRequest(req)) {
      setSseHeaders(res);
      const runAbortController = new AbortController();
      portalRunAbortControllers.set(runId, runAbortController);
      let streamClosed = false;
      let sawAssistantDelta = false;
      let streamedApproval: PortalApprovalSummary | undefined;
      let streamedApprovalRequiredEvent: PortalRuntimeEventWire | undefined;
      let unsubscribe = () => {};
      const closeStream = () => {
        if (streamClosed) {
          return;
        }
        streamClosed = true;
        unsubscribe();
        writeDone(res);
        res.end();
      };
      unsubscribe = onAgentEvent((evt) => {
        if (streamClosed || evt.runId !== runId) {
          return;
        }
        if (evt.stream === "assistant") {
          const delta = resolveAssistantStreamDeltaText(evt);
          if (!delta) {
            return;
          }
          sawAssistantDelta = true;
          const currentRun = portalRuns.get(runId);
          if (currentRun && !currentRun.endedAt) {
            const nextReply = `${currentRun.reply ?? ""}${delta}`;
            savePortalRun({
              ...currentRun,
              reply: nextReply,
              streamSeq: (currentRun.streamSeq ?? 0) + 1,
              replyUpdatedAt: new Date(evt.ts).toISOString(),
            });
          }
          writePortalStreamEvent(res, "assistant.delta", {
            runId,
            traceId: traceId ?? nextSession.traceId ?? null,
            delta,
            seq: evt.seq,
            createdAt: new Date(evt.ts).toISOString(),
          });
          return;
        }
        if (nextSession.mode === "training") {
          const approvalFromTool = buildPortalApprovalSummaryFromAgentEvent(evt);
          if (approvalFromTool && streamedApproval?.id !== approvalFromTool.id) {
            streamedApproval = approvalFromTool;
            streamedApprovalRequiredEvent = buildPortalApprovalRequiredEvent({
              runId,
              approval: approvalFromTool,
              createdAt: new Date(evt.ts).toISOString(),
            });
            writePortalStreamEvent(res, "runtime.event", streamedApprovalRequiredEvent);
          }
        }
        const runtimeEvent = buildPortalRuntimeEventFromAgentEvent(evt);
        if (!runtimeEvent) {
          return;
        }
        writePortalStreamEvent(res, "runtime.event", runtimeEvent);
      });
      // For POST + SSE, the request stream can close once the body is fully read.
      // We only want to stop forwarding when the response stream is actually closed.
      res.on("close", () => {
        if (streamClosed) {
          return;
        }
        streamClosed = true;
        unsubscribe();
      });

      writePortalStreamEvent(res, "message.start", {
        runId,
        traceId: traceId ?? nextSession.traceId ?? null,
        portalSessionId: portalSessionId ?? nextSession.portalSessionId ?? null,
        remoteSessionId,
        mode: nextSession.mode,
        conversationView: nextSession.conversationView,
        runtimeRole: nextSession.runtimeRole,
        agentVersionId: nextSession.agentVersionId ?? null,
        releaseId: nextSession.releaseId ?? null,
        releaseVersion: nextSession.releaseVersion ?? null,
        releaseStatus: nextSession.releaseStatus ?? null,
        startedAt: runStartedAt,
      });
      writePortalStreamEvent(res, "runtime.event", runStartedEvent);

      void (async () => {
        try {
          const cfg = loadConfig();
          const workspaceDir = resolveAgentWorkspaceDir(cfg, nextSession.agentId);
          const deliverablesPortalSessionId =
            portalSessionId ?? nextSession.portalSessionId ?? remoteSessionId;
          await cleanupExpiredPortalDeliverablesForSession(
            workspaceDir,
            deliverablesPortalSessionId,
          );
          await fs.mkdir(
            resolvePortalRunDeliverablesRoot(workspaceDir, deliverablesPortalSessionId, runId),
            {
              recursive: true,
              mode: 0o700,
            },
          );
          const sharedFiles = await listPortalSharedFiles(workspaceDir);
          const result = await agentCommandFromIngress(
            {
              message: effectiveMessage,
              images: parsedImages.length > 0 ? parsedImages : undefined,
              imageOrder: parsedImageOrder.length > 0 ? parsedImageOrder : undefined,
              sessionKey: nextSession.sessionKey,
              runId,
              deliver: false,
              messageChannel: "webchat",
              bestEffortDeliver: false,
              senderIsOwner: true,
              allowModelOverride: false,
              thinking: "off",
              extraSystemPrompt: buildPortalExtraSystemPrompt({
                remoteSessionId,
                session: nextSession,
                traceId,
                portalSessionId,
                runId,
                skillSearchPrefetch,
                sharedFiles,
                memoryContextPrompt,
                memoryPolicy,
              }),
              portalContext: buildPortalPluginContext({
                session: nextSession,
                portalSessionId,
                traceId,
              }),
              abortSignal: runAbortController.signal,
            },
            defaultRuntime,
            createDefaultDeps(),
          );
          const reply = resolvePortalReplyText(result);
          const usage = extractPortalUsage(result);
          const outputAttachments = await resolvePortalMessageAttachments({
            workspaceDir,
            portalSessionId: deliverablesPortalSessionId,
            runId,
            reply,
          });
          if (reply === EMPTY_PORTAL_REPLY) {
            defaultRuntime.log(
              `[control-plane] portal session produced no visible reply (runId=${runId}, traceId=${traceId}, remoteSessionId=${remoteSessionId}, agentId=${nextSession.agentId})`,
            );
          }
          let approval: PortalApprovalSummary | undefined = streamedApproval;
          if (nextSession.mode === "training") {
            const manager = getGlobalExecApprovalManager();
            if (manager) {
              const pendingForSession = listPendingExecApprovalRecords(manager).filter(
                (record: ExecApprovalRecord) => {
                  const requestSessionKey =
                    record.request.sessionKey ??
                    record.request.systemRunBinding?.sessionKey ??
                    null;
                  return requestSessionKey === nextSession.sessionKey;
                },
              );
              if (pendingForSession.length > 0) {
                let latest = pendingForSession[0];
                for (const current of pendingForSession.slice(1)) {
                  if (current.createdAtMs > latest.createdAtMs) {
                    latest = current;
                  }
                }
                approval = mergePortalApprovalSummary(
                  approval,
                  buildPortalApprovalSummaryFromRecord(latest),
                );
              }
            }
          }
          const candidateChanges = buildTrainingCandidateChanges({
            session: nextSession,
            message,
            reply,
            status: approval ? "requires_approval" : "completed",
            approval,
          });
          const updatedHistorySummary = appendHistorySummary(
            nextSession.historySummary,
            summarizePortalExchange({
              message,
              reply,
              usage,
            }),
          );

          if (!streamClosed && !sawAssistantDelta && reply && reply !== EMPTY_PORTAL_REPLY) {
            sawAssistantDelta = true;
            writePortalStreamEvent(res, "assistant.delta", {
              runId,
              traceId: traceId ?? nextSession.traceId ?? null,
              delta: reply,
              createdAt: new Date().toISOString(),
            });
          }

          if (approval) {
            portalSessions.set(remoteSessionId, {
              ...nextSession,
              turnCount: nextSession.turnCount + 1,
              historySummary: updatedHistorySummary,
            });
            const approvalTime = new Date().toISOString();
            const runRecord = savePortalRun({
              runId,
              remoteSessionId,
              portalSessionId: portalSessionId ?? nextSession.portalSessionId,
              traceId: traceId ?? nextSession.traceId,
              status: "requires_approval",
              startedAt: runStartedAt,
              endedAt: approvalTime,
              durationMs: Math.max(0, Date.parse(approvalTime) - Date.parse(runStartedAt)),
              reply,
              usage,
              attachments: outputAttachments,
              candidateChanges,
              timeline: appendPortalRunTimeline(portalRuns.get(runId), {
                phase: "requires_approval",
                at: approvalTime,
              }),
            });
            const approvalRequiredEvent =
              streamedApprovalRequiredEvent ??
              buildPortalApprovalRequiredEvent({
                runId,
                approval,
                createdAt: approvalTime,
              });
            streamedApprovalRequiredEvent = approvalRequiredEvent;
            if (!streamClosed && !streamedApproval) {
              writePortalStreamEvent(res, "runtime.event", approvalRequiredEvent);
            }
            if (!streamClosed) {
              writePortalStreamEvent(res, "message.complete", {
                ok: true,
                status: "requires_approval",
                runId,
                traceId: traceId ?? nextSession.traceId,
                portalSessionId: portalSessionId ?? nextSession.portalSessionId,
                remoteSessionId,
                startedAt: runRecord.startedAt,
                endedAt: runRecord.endedAt,
                durationMs: runRecord.durationMs,
                reply,
                usage,
                mode: nextSession.mode,
                conversationView: nextSession.conversationView,
                runtimeRole: nextSession.runtimeRole,
                writePolicy: nextSession.writePolicy,
                agentVersionId: nextSession.agentVersionId,
                releaseId: nextSession.releaseId,
                releaseVersion: nextSession.releaseVersion,
                releaseStatus: nextSession.releaseStatus,
                timeline: runRecord.timeline,
                approval,
                candidateChanges,
                attachments: outputAttachments,
                runtimeEvents: [runStartedEvent, approvalRequiredEvent],
              });
            }
          } else {
            let persistedSession: PortalSessionRecord = {
              ...nextSession,
              turnCount: nextSession.turnCount + 1,
              historySummary: updatedHistorySummary,
            };
            if (shouldRolloverPortalSession({ session: persistedSession, usage })) {
              const nextRevision = persistedSession.sessionRevision + 1;
              persistedSession = {
                ...persistedSession,
                sessionRevision: nextRevision,
                sessionKey: buildPortalSessionKey({
                  cfg,
                  agentId: persistedSession.agentId,
                  remoteSessionId,
                  conversationView: persistedSession.conversationView,
                  revision: nextRevision,
                }),
                turnCount: 0,
              };
            }
            portalSessions.set(remoteSessionId, persistedSession);
            await prunePortalRunDeliverablesRoot(workspaceDir, deliverablesPortalSessionId, runId);
            const completedAt = new Date().toISOString();
            const runRecord = savePortalRun({
              runId,
              remoteSessionId,
              portalSessionId: portalSessionId ?? nextSession.portalSessionId,
              traceId: traceId ?? nextSession.traceId,
              status: "completed",
              startedAt: runStartedAt,
              endedAt: completedAt,
              durationMs: Math.max(0, Date.parse(completedAt) - Date.parse(runStartedAt)),
              reply,
              usage,
              attachments: outputAttachments,
              candidateChanges,
              timeline: appendPortalRunTimeline(portalRuns.get(runId), {
                phase: "completed",
                at: completedAt,
              }),
            });
            const runCompletedEvent = buildPortalRuntimeEvent({
              eventType: "run.completed",
              level: "info",
              message: `Portal run ${runId} completed`,
              payload: {
                runId,
                status: "completed",
                usage: usage ?? null,
              },
              createdAt: completedAt,
            });
            if (!streamClosed) {
              writePortalStreamEvent(res, "runtime.event", runCompletedEvent);
              writePortalStreamEvent(res, "message.complete", {
                ok: true,
                status: "completed",
                runId,
                traceId: traceId ?? nextSession.traceId,
                portalSessionId: portalSessionId ?? nextSession.portalSessionId,
                remoteSessionId,
                startedAt: runRecord.startedAt,
                endedAt: runRecord.endedAt,
                durationMs: runRecord.durationMs,
                reply,
                usage,
                mode: nextSession.mode,
                conversationView: nextSession.conversationView,
                runtimeRole: nextSession.runtimeRole,
                writePolicy: nextSession.writePolicy,
                agentVersionId: nextSession.agentVersionId,
                releaseId: nextSession.releaseId,
                releaseVersion: nextSession.releaseVersion,
                releaseStatus: nextSession.releaseStatus,
                timeline: runRecord.timeline,
                attachments: outputAttachments,
                candidateChanges,
                runtimeEvents: [runStartedEvent, runCompletedEvent],
              });
            }
          }
        } catch (error) {
          const failedAt = new Date().toISOString();
          const message = error instanceof Error ? error.message : String(error);
          const aborted = runAbortController.signal.aborted;
          const failureCode = aborted ? "PORTAL_RUN_ABORTED" : "PORTAL_RUN_FAILED";
          const failureStatus = aborted ? "stopped" : "failed";
          const failureMessage = aborted ? "已暂停当前执行" : message;
          try {
            const cfg = loadConfig();
            const workspaceDir = resolveAgentWorkspaceDir(cfg, nextSession.agentId);
            const deliverablesPortalSessionId =
              portalSessionId ?? nextSession.portalSessionId ?? remoteSessionId;
            await prunePortalRunDeliverablesRoot(workspaceDir, deliverablesPortalSessionId, runId);
          } catch {}
          const runRecord = savePortalRun({
            runId,
            remoteSessionId,
            portalSessionId: portalSessionId ?? nextSession.portalSessionId,
            traceId: traceId ?? nextSession.traceId,
            status: failureStatus,
            startedAt: runStartedAt,
            endedAt: failedAt,
            durationMs: Math.max(0, Date.parse(failedAt) - Date.parse(runStartedAt)),
            error: {
              message: failureMessage,
              code: failureCode,
            },
            timeline: appendPortalRunTimeline(portalRuns.get(runId), {
              phase: aborted ? "stopped" : "failed",
              at: failedAt,
              error: failureMessage,
            }),
          });
          const runFailedEvent = buildPortalRuntimeEvent({
            eventType: aborted ? "run.stopped" : "run.failed",
            level: aborted ? "warn" : "error",
            message: aborted ? `Portal run ${runId} stopped` : `Portal run ${runId} failed`,
            payload: {
              runId,
              code: failureCode,
              error: failureMessage,
            },
            createdAt: failedAt,
          });
          if (!streamClosed) {
            writePortalStreamEvent(res, "runtime.event", runFailedEvent);
            writePortalStreamEvent(res, "message.error", {
              code: failureCode,
              message: failureMessage,
              status: aborted ? 409 : 500,
              runId,
              traceId: traceId ?? nextSession.traceId,
              portalSessionId: portalSessionId ?? nextSession.portalSessionId,
              remoteSessionId,
              startedAt: runRecord.startedAt,
              endedAt: runRecord.endedAt,
              durationMs: runRecord.durationMs,
              timeline: runRecord.timeline,
              usage: { inputTokens: 0, outputTokens: 0, totalTokens: 0 },
              runtimeEvents: [runStartedEvent, runFailedEvent],
            });
          }
        } finally {
          portalRunAbortControllers.delete(runId);
          if (!streamClosed) {
            closeStream();
          }
        }
      })();
      return true;
    }
    const runAbortController = new AbortController();
    portalRunAbortControllers.set(runId, runAbortController);
    try {
      const cfg = loadConfig();
      const workspaceDir = resolveAgentWorkspaceDir(cfg, nextSession.agentId);
      const deliverablesPortalSessionId =
        portalSessionId ?? nextSession.portalSessionId ?? remoteSessionId;
      await cleanupExpiredPortalDeliverablesForSession(workspaceDir, deliverablesPortalSessionId);
      await fs.mkdir(
        resolvePortalRunDeliverablesRoot(workspaceDir, deliverablesPortalSessionId, runId),
        {
          recursive: true,
          mode: 0o700,
        },
      );
      const sharedFiles = await listPortalSharedFiles(workspaceDir);
      const result = await agentCommandFromIngress(
        {
          message: effectiveMessage,
          images: parsedImages.length > 0 ? parsedImages : undefined,
          imageOrder: parsedImageOrder.length > 0 ? parsedImageOrder : undefined,
          sessionKey: nextSession.sessionKey,
          runId,
          deliver: false,
          messageChannel: "webchat",
          bestEffortDeliver: false,
          senderIsOwner: true,
          allowModelOverride: false,
          thinking: "off",
          extraSystemPrompt: buildPortalExtraSystemPrompt({
            remoteSessionId,
            session: nextSession,
            traceId,
            portalSessionId,
            runId,
            skillSearchPrefetch,
            sharedFiles,
            memoryContextPrompt,
            memoryPolicy,
          }),
          portalContext: buildPortalPluginContext({
            session: nextSession,
            portalSessionId,
            traceId,
          }),
          abortSignal: runAbortController.signal,
        },
        defaultRuntime,
        createDefaultDeps(),
      );
      const reply = resolvePortalReplyText(result);
      const usage = extractPortalUsage(result);
      const outputAttachments = await resolvePortalMessageAttachments({
        workspaceDir,
        portalSessionId: deliverablesPortalSessionId,
        runId,
        reply,
      });
      if (reply === EMPTY_PORTAL_REPLY) {
        defaultRuntime.log(
          `[control-plane] portal session produced no visible reply (runId=${runId}, traceId=${traceId}, remoteSessionId=${remoteSessionId}, agentId=${nextSession.agentId})`,
        );
      }
      let approval: PortalApprovalSummary | undefined;
      if (nextSession.mode === "training") {
        const manager = getGlobalExecApprovalManager();
        if (manager) {
          const pendingForSession = listPendingExecApprovalRecords(manager).filter(
            (record: ExecApprovalRecord) => {
              const requestSessionKey =
                record.request.sessionKey ?? record.request.systemRunBinding?.sessionKey ?? null;
              return requestSessionKey === nextSession.sessionKey;
            },
          );
          if (pendingForSession.length > 0) {
            let latest = pendingForSession[0];
            for (const current of pendingForSession.slice(1)) {
              if (current.createdAtMs > latest.createdAtMs) {
                latest = current;
              }
            }
            approval = {
              id: latest.id,
              kind: "exec",
              command: latest.request.command,
              host: latest.request.host ?? undefined,
              cwd: latest.request.cwd ?? undefined,
              expiresAt: new Date(latest.expiresAtMs).toISOString(),
            };
          }
        }
      }
      const candidateChanges = buildTrainingCandidateChanges({
        session: nextSession,
        message,
        reply,
        status: approval ? "requires_approval" : "completed",
        approval,
      });
      const updatedHistorySummary = appendHistorySummary(
        nextSession.historySummary,
        summarizePortalExchange({
          message,
          reply,
          usage,
        }),
      );
      if (approval) {
        portalSessions.set(remoteSessionId, {
          ...nextSession,
          turnCount: nextSession.turnCount + 1,
          historySummary: updatedHistorySummary,
        });
        const approvalTime = new Date().toISOString();
        const runRecord = savePortalRun({
          runId,
          remoteSessionId,
          portalSessionId: portalSessionId ?? nextSession.portalSessionId,
          traceId: traceId ?? nextSession.traceId,
          status: "requires_approval",
          startedAt: runStartedAt,
          endedAt: approvalTime,
          durationMs: Math.max(0, Date.parse(approvalTime) - Date.parse(runStartedAt)),
          reply,
          usage,
          attachments: outputAttachments,
          candidateChanges,
          timeline: appendPortalRunTimeline(portalRuns.get(runId), {
            phase: "requires_approval",
            at: approvalTime,
          }),
        });
        const approvalRequiredEvent = buildPortalRuntimeEvent({
          eventType: "approval.required",
          level: "warn",
          message: "Exec approval required before continuing",
          payload: {
            runId,
            approvalId: approval.id,
            kind: approval.kind,
            command: approval.command,
            host: approval.host ?? null,
            cwd: approval.cwd ?? null,
            expiresAt: approval.expiresAt ?? null,
          },
          createdAt: approvalTime,
        });
        sendJson(res, 200, {
          ok: true,
          status: "requires_approval",
          runId,
          traceId: traceId ?? nextSession.traceId,
          portalSessionId: portalSessionId ?? nextSession.portalSessionId,
          remoteSessionId,
          startedAt: runRecord.startedAt,
          endedAt: runRecord.endedAt,
          durationMs: runRecord.durationMs,
          reply,
          usage,
          mode: nextSession.mode,
          conversationView: nextSession.conversationView,
          runtimeRole: nextSession.runtimeRole,
          writePolicy: nextSession.writePolicy,
          agentVersionId: nextSession.agentVersionId,
          releaseId: nextSession.releaseId,
          releaseVersion: nextSession.releaseVersion,
          releaseStatus: nextSession.releaseStatus,
          timeline: runRecord.timeline,
          approval,
          candidateChanges,
          attachments: outputAttachments,
          runtimeEvents: [runStartedEvent, approvalRequiredEvent],
        });
      } else {
        let persistedSession: PortalSessionRecord = {
          ...nextSession,
          turnCount: nextSession.turnCount + 1,
          historySummary: updatedHistorySummary,
        };
        if (shouldRolloverPortalSession({ session: persistedSession, usage })) {
          const nextRevision = persistedSession.sessionRevision + 1;
          persistedSession = {
            ...persistedSession,
            sessionRevision: nextRevision,
            sessionKey: buildPortalSessionKey({
              cfg,
              agentId: persistedSession.agentId,
              remoteSessionId,
              conversationView: persistedSession.conversationView,
              revision: nextRevision,
            }),
            turnCount: 0,
          };
        }
        portalSessions.set(remoteSessionId, persistedSession);
        await prunePortalRunDeliverablesRoot(workspaceDir, deliverablesPortalSessionId, runId);
        const completedAt = new Date().toISOString();
        const runRecord = savePortalRun({
          runId,
          remoteSessionId,
          portalSessionId: portalSessionId ?? nextSession.portalSessionId,
          traceId: traceId ?? nextSession.traceId,
          status: "completed",
          startedAt: runStartedAt,
          endedAt: completedAt,
          durationMs: Math.max(0, Date.parse(completedAt) - Date.parse(runStartedAt)),
          reply,
          usage,
          attachments: outputAttachments,
          candidateChanges,
          timeline: appendPortalRunTimeline(portalRuns.get(runId), {
            phase: "completed",
            at: completedAt,
          }),
        });
        const runCompletedEvent = buildPortalRuntimeEvent({
          eventType: "run.completed",
          level: "info",
          message: `Portal run ${runId} completed`,
          payload: {
            runId,
            status: "completed",
            usage: usage ?? null,
          },
          createdAt: completedAt,
        });
        sendJson(res, 200, {
          ok: true,
          status: "completed",
          runId,
          traceId: traceId ?? nextSession.traceId,
          portalSessionId: portalSessionId ?? nextSession.portalSessionId,
          remoteSessionId,
          startedAt: runRecord.startedAt,
          endedAt: runRecord.endedAt,
          durationMs: runRecord.durationMs,
          reply,
          usage,
          mode: nextSession.mode,
          conversationView: nextSession.conversationView,
          runtimeRole: nextSession.runtimeRole,
          writePolicy: nextSession.writePolicy,
          agentVersionId: nextSession.agentVersionId,
          releaseId: nextSession.releaseId,
          releaseVersion: nextSession.releaseVersion,
          releaseStatus: nextSession.releaseStatus,
          timeline: runRecord.timeline,
          attachments: outputAttachments,
          candidateChanges,
          runtimeEvents: [runStartedEvent, runCompletedEvent],
        });
      }
    } catch (error) {
      const failedAt = new Date().toISOString();
      const message = error instanceof Error ? error.message : String(error);
      const aborted = runAbortController.signal.aborted;
      const failureCode = aborted ? "PORTAL_RUN_ABORTED" : "PORTAL_RUN_FAILED";
      const failureStatus = aborted ? "stopped" : "failed";
      const failureMessage = aborted ? "已暂停当前执行" : message;
      const cfg = loadConfig();
      const workspaceDir = resolveAgentWorkspaceDir(cfg, nextSession.agentId);
      const deliverablesPortalSessionId =
        portalSessionId ?? nextSession.portalSessionId ?? remoteSessionId;
      await prunePortalRunDeliverablesRoot(workspaceDir, deliverablesPortalSessionId, runId).catch(
        () => undefined,
      );
      const runRecord = savePortalRun({
        runId,
        remoteSessionId,
        portalSessionId: portalSessionId ?? nextSession.portalSessionId,
        traceId: traceId ?? nextSession.traceId,
        status: failureStatus,
        startedAt: runStartedAt,
        endedAt: failedAt,
        durationMs: Math.max(0, Date.parse(failedAt) - Date.parse(runStartedAt)),
        error: {
          message: failureMessage,
          code: failureCode,
        },
        timeline: appendPortalRunTimeline(portalRuns.get(runId), {
          phase: aborted ? "stopped" : "failed",
          at: failedAt,
          error: failureMessage,
        }),
      });
      const runFailedEvent = buildPortalRuntimeEvent({
        eventType: aborted ? "run.stopped" : "run.failed",
        level: aborted ? "warn" : "error",
        message: aborted ? `Portal run ${runId} stopped` : `Portal run ${runId} failed`,
        payload: {
          runId,
          code: failureCode,
          error: failureMessage,
        },
        createdAt: failedAt,
      });
      sendJson(res, aborted ? 409 : 500, {
        ok: false,
        error: failureMessage,
        code: failureCode,
        status: failureStatus,
        runId,
        traceId: traceId ?? nextSession.traceId,
        portalSessionId: portalSessionId ?? nextSession.portalSessionId,
        remoteSessionId,
        startedAt: runRecord.startedAt,
        endedAt: runRecord.endedAt,
        durationMs: runRecord.durationMs,
        timeline: runRecord.timeline,
        usage: { inputTokens: 0, outputTokens: 0, totalTokens: 0 },
        runtimeEvents: [runStartedEvent, runFailedEvent],
      });
    } finally {
      portalRunAbortControllers.delete(runId);
    }
    return true;
  }

  const portalRunMatch = url.pathname.match(
    new RegExp(`^${PREFIX.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}/portal/runs/([^/]+)$`),
  );
  if (portalRunMatch) {
    if (!ensureMethod(req, res, "GET")) {
      return true;
    }
    const runId = portalRunMatch[1] ?? "";
    const run = portalRuns.get(runId);
    if (!run) {
      sendJson(res, 404, { ok: false, error: "run not found", runId });
      return true;
    }
    sendJson(res, 200, {
      ok: true,
      ...run,
    });
    return true;
  }

  const portalApprovalDecisionMatch = url.pathname.match(
    new RegExp(
      `^${PREFIX.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}/portal/sessions/([^/]+)/approvals/([^/]+)/decision$`,
    ),
  );
  if (portalApprovalDecisionMatch) {
    if (!ensureMethod(req, res, "POST")) {
      return true;
    }
    const remoteSessionId = portalApprovalDecisionMatch[1];
    const approvalId = portalApprovalDecisionMatch[2];
    const session = portalSessions.get(remoteSessionId);
    if (!session) {
      sendJson(res, 404, {
        ok: false,
        error: "session not found",
        remoteSessionId,
      });
      return true;
    }
    if (session.mode !== "training") {
      sendJson(res, 403, {
        ok: false,
        error: "exec approvals can only be resolved for training-mode portal sessions",
        mode: session.mode,
      });
      return true;
    }
    const body = await readBody(req);
    const decisionRaw = typeof body.decision === "string" ? body.decision.trim().toLowerCase() : "";
    if (decisionRaw !== "allow-once" && decisionRaw !== "allow-always" && decisionRaw !== "deny") {
      sendJson(res, 400, { ok: false, error: "invalid decision" });
      return true;
    }
    const manager = getGlobalExecApprovalManager();
    if (!manager) {
      sendJson(res, 503, { ok: false, error: "exec approvals unavailable" });
      return true;
    }
    const snapshot = manager.getSnapshot(approvalId);
    if (!snapshot) {
      sendJson(res, 400, { ok: false, error: "approval expired" });
      return true;
    }
    const requestSessionKey =
      snapshot.request.sessionKey ?? snapshot.request.systemRunBinding?.sessionKey ?? null;
    if (requestSessionKey && requestSessionKey !== session.sessionKey) {
      sendJson(res, 403, {
        ok: false,
        error: "approval does not belong to this portal session",
      });
      return true;
    }
    if (snapshot.resolvedAtMs !== undefined) {
      sendJson(res, 400, { ok: false, error: "approval expired" });
      return true;
    }
    const resolvedBy = "portal.control-plane";
    const okResolve = manager.resolve(approvalId, decisionRaw as ExecApprovalDecision, resolvedBy);
    if (!okResolve) {
      sendJson(res, 400, { ok: false, error: "approval expired" });
      return true;
    }
    const broadcast = getGlobalExecApprovalBroadcast();
    const forwarder = getGlobalExecApprovalForwarder();
    const ts = Date.now();
    const lastRunId = session.lastRunId;
    const existingRun = lastRunId ? portalRuns.get(lastRunId) : undefined;
    const resolvedAt = new Date(ts).toISOString();
    const updatedRun =
      lastRunId && existingRun
        ? savePortalRun({
            ...existingRun,
            status: "approval_applied",
            timeline: appendPortalRunTimeline(existingRun, {
              phase: "approval_applied",
              at: resolvedAt,
            }),
          })
        : undefined;
    const approvalAppliedEvent = buildPortalRuntimeEvent({
      eventType: "approval.applied",
      level: decisionRaw === "deny" ? "warn" : "info",
      message: `审批已处理：${decisionRaw}`,
      payload: {
        runId: lastRunId ?? null,
        approvalId,
        decision: decisionRaw,
      },
      createdAt: resolvedAt,
    });
    if (isSseRequest(req)) {
      setSseHeaders(res);
      let streamClosed = false;
      let unsubscribe = () => {};
      const streamedRuntimeEvents: PortalRuntimeEventWire[] = [approvalAppliedEvent];
      const bufferedDeltas: string[] = [];
      let idleTimer: ReturnType<typeof setTimeout> | null = null;
      const clearIdleTimer = () => {
        if (idleTimer) {
          clearTimeout(idleTimer);
          idleTimer = null;
        }
      };
      const finishStream = async (
        kind: "complete" | "error",
        override?: Record<string, unknown>,
      ) => {
        if (streamClosed) {
          return;
        }
        streamClosed = true;
        clearIdleTimer();
        unsubscribe();
        let currentRun = lastRunId ? (portalRuns.get(lastRunId) ?? updatedRun) : updatedRun;
        const continuedReply = bufferedDeltas.join("");
        const priorReply = currentRun?.reply ?? existingRun?.reply ?? "";
        const combinedReply =
          continuedReply.trim().length > 0
            ? [priorReply, continuedReply].filter(Boolean).join("\n\n").trim()
            : priorReply || undefined;
        let outputAttachments = currentRun?.attachments ?? [];
        if (lastRunId) {
          try {
            const cfg = loadConfig();
            const workspaceDir = resolveAgentWorkspaceDir(cfg, session.agentId);
            const deliverablesPortalSessionId = session.portalSessionId ?? remoteSessionId;
            outputAttachments = await resolvePortalMessageAttachments({
              workspaceDir,
              portalSessionId: deliverablesPortalSessionId,
              runId: lastRunId,
              reply: combinedReply,
            });
            await prunePortalRunDeliverablesRoot(
              workspaceDir,
              deliverablesPortalSessionId,
              lastRunId,
            );
            if (currentRun) {
              currentRun = savePortalRun({
                ...currentRun,
                attachments: outputAttachments,
              });
            }
          } catch {
            outputAttachments = currentRun?.attachments ?? [];
          }
        }
        const payload = {
          decision: decisionRaw,
          status:
            override?.status ??
            (kind === "error"
              ? "failed"
              : combinedReply || currentRun?.status === "completed"
                ? "completed"
                : "applied"),
          runId: lastRunId ?? null,
          traceId: session.traceId,
          remoteSessionId,
          portalSessionId: session.portalSessionId ?? null,
          startedAt: currentRun?.startedAt,
          endedAt: currentRun?.endedAt ?? resolvedAt,
          durationMs: currentRun?.durationMs,
          reply: combinedReply ?? "",
          usage: currentRun?.usage,
          attachments: outputAttachments,
          timeline: currentRun?.timeline,
          runtimeEvents: streamedRuntimeEvents,
          ...override,
        };
        writePortalStreamEvent(
          res,
          kind === "error" ? "message.error" : "message.complete",
          payload,
        );
        writeDone(res);
        res.end();
      };
      const touchIdleTimer = () => {
        clearIdleTimer();
        idleTimer = setTimeout(() => {
          void finishStream("complete");
        }, 15000);
      };
      // Keep the approval continuation stream alive until the response closes.
      res.on("close", () => {
        streamClosed = true;
        clearIdleTimer();
        unsubscribe();
      });
      if (lastRunId) {
        unsubscribe = onAgentEvent((evt) => {
          if (streamClosed || evt.runId !== lastRunId) {
            return;
          }
          touchIdleTimer();
          if (evt.stream === "assistant") {
            const delta = resolveAssistantStreamDeltaText(evt);
            if (!delta) {
              return;
            }
            bufferedDeltas.push(delta);
            const currentRun = portalRuns.get(lastRunId) ?? updatedRun ?? existingRun;
            if (currentRun && lastRunId && !currentRun.endedAt) {
              const nextReply = `${currentRun.reply ?? ""}${delta}`;
              savePortalRun({
                ...currentRun,
                reply: nextReply,
                streamSeq: (currentRun.streamSeq ?? 0) + 1,
                replyUpdatedAt: new Date(evt.ts).toISOString(),
              });
            }
            writePortalStreamEvent(res, "assistant.delta", {
              runId: lastRunId,
              traceId: session.traceId ?? null,
              delta,
              seq: evt.seq,
              createdAt: new Date(evt.ts).toISOString(),
            });
            return;
          }
          const runtimeEvent = buildPortalRuntimeEventFromAgentEvent(evt);
          if (runtimeEvent) {
            streamedRuntimeEvents.push(runtimeEvent);
            writePortalStreamEvent(res, "runtime.event", runtimeEvent);
          }
          if (evt.stream === "lifecycle" && typeof evt.data?.phase === "string") {
            const lifecyclePhase = evt.data.phase;
            if (lifecyclePhase === "end" || lifecyclePhase === "error") {
              const current = portalRuns.get(lastRunId) ?? updatedRun ?? existingRun;
              if (current) {
                const endedAt = new Date(evt.ts).toISOString();
                const nextRun = savePortalRun({
                  ...current,
                  status: lifecyclePhase === "error" ? "failed" : "completed",
                  endedAt,
                  durationMs: Math.max(0, Date.parse(endedAt) - Date.parse(current.startedAt)),
                  reply:
                    [current.reply, bufferedDeltas.join("")]
                      .filter((part) => typeof part === "string" && part.trim().length > 0)
                      .join("\n\n") || current.reply,
                  timeline: appendPortalRunTimeline(current, {
                    phase: lifecyclePhase === "error" ? "failed" : "completed",
                    at: endedAt,
                    error:
                      lifecyclePhase === "error" && typeof evt.data?.error === "string"
                        ? evt.data.error
                        : undefined,
                  }),
                });
                portalRuns.set(lastRunId, nextRun);
              }
              void finishStream(
                lifecyclePhase === "error" ? "error" : "complete",
                lifecyclePhase === "error"
                  ? {
                      code: "PORTAL_RUN_FAILED",
                      message:
                        typeof evt.data?.error === "string" ? evt.data.error : "审批后续执行失败",
                    }
                  : undefined,
              );
            }
          }
        });
      }
      writePortalStreamEvent(res, "runtime.event", approvalAppliedEvent);
      if (decisionRaw === "deny" || !lastRunId) {
        void finishStream("complete", {
          status: decisionRaw === "deny" ? "denied" : "applied",
          timeline: updatedRun?.timeline,
        });
      } else {
        touchIdleTimer();
      }
    }
    if (broadcast) {
      broadcast(
        "exec.approval.resolved",
        {
          id: approvalId,
          decision: decisionRaw,
          resolvedBy,
          ts,
          request: snapshot.request,
        },
        { dropIfSlow: true },
      );
    }
    if (forwarder) {
      void forwarder
        .handleResolved({
          id: approvalId,
          decision: decisionRaw as ExecApprovalDecision,
          resolvedBy,
          ts,
          request: snapshot.request,
        })
        .catch((err) => {
          defaultRuntime.log(
            `[control-plane] exec approvals: forward resolve failed: ${String(err)}`,
          );
        });
    }
    if (isSseRequest(req)) {
      return true;
    }
    sendJson(res, 200, {
      ok: true,
      runId: lastRunId,
      traceId: session.traceId,
      remoteSessionId,
      portalSessionId: session.portalSessionId,
      status: "applied",
      timeline: updatedRun?.timeline,
    });
    return true;
  }

  sendJson(res, 404, { error: "not found" });
  return true;
}
