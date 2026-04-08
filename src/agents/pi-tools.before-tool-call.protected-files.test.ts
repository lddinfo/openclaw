import { beforeEach, describe, expect, it, vi } from "vitest";
import { resetDiagnosticSessionStateForTest } from "../logging/diagnostic-session-state.js";
import { getGlobalHookRunner } from "../plugins/hook-runner-global.js";
import { runBeforeToolCallHook } from "./pi-tools.before-tool-call.js";

vi.mock("../plugins/hook-runner-global.js", async () => {
  const actual = await vi.importActual<typeof import("../plugins/hook-runner-global.js")>(
    "../plugins/hook-runner-global.js",
  );
  return {
    ...actual,
    getGlobalHookRunner: vi.fn(),
  };
});

const mockGetGlobalHookRunner = vi.mocked(getGlobalHookRunner);

describe("before_tool_call protected workspace files", () => {
  beforeEach(() => {
    resetDiagnosticSessionStateForTest();
    mockGetGlobalHookRunner.mockReturnValue({
      hasHooks: vi.fn().mockReturnValue(false),
      runBeforeToolCall: vi.fn(),
    } as any);
  });

  const workspaceDir = "/tmp/openclaw-agent";
  const servingCtx = {
    agentId: "main",
    sessionKey: "agent:main:portal:serving:rs_1",
    workspaceDir,
    portalContext: {
      mode: "chat",
      conversationView: "serving",
      writePolicy: {
        core: "forbidden",
        memory: "user-memory",
      },
    },
  } as const;

  it("blocks write to protected root files for non-training portal sessions", async () => {
    const result = await runBeforeToolCallHook({
      toolName: "write",
      params: { path: "AGENTS.md", content: "blocked" },
      ctx: servingCtx,
    });

    expect(result).toEqual({
      blocked: true,
      reason: "Non-training portal sessions cannot modify protected agent files: AGENTS.md.",
    });
  });

  it("blocks protected files for serving portal session keys even without portalContext", async () => {
    const result = await runBeforeToolCallHook({
      toolName: "edit",
      params: { path: "AGENTS.md", oldText: "a", newText: "b" },
      ctx: {
        agentId: "main",
        sessionKey: "agent:main:portal:serving:rs_2",
        workspaceDir,
      },
    });

    expect(result).toEqual({
      blocked: true,
      reason: "Non-training portal sessions cannot modify protected agent files: AGENTS.md.",
    });
  });

  it("blocks apply_patch touching protected root files for non-training portal sessions", async () => {
    const result = await runBeforeToolCallHook({
      toolName: "apply_patch",
      params: {
        input: [
          "*** Begin Patch",
          "*** Update File: IDENTITY.md",
          "@@",
          "-old",
          "+new",
          "*** End Patch",
        ].join("\n"),
      },
      ctx: servingCtx,
    });

    expect(result).toEqual({
      blocked: true,
      reason: "Non-training portal sessions cannot modify protected agent files: IDENTITY.md.",
    });
  });

  it("allows protected file edits during training sessions", async () => {
    const result = await runBeforeToolCallHook({
      toolName: "write",
      params: { path: "SOUL.md", content: "allowed" },
      ctx: {
        ...servingCtx,
        portalContext: {
          mode: "training",
          conversationView: "training",
          writePolicy: {
            core: "candidate-core",
            memory: "candidate-core",
          },
        },
      },
    });

    expect(result).toEqual({
      blocked: false,
      params: { path: "SOUL.md", content: "allowed" },
    });
  });

  it("allows non-protected files for non-training portal sessions", async () => {
    const result = await runBeforeToolCallHook({
      toolName: "write",
      params: { path: "notes.md", content: "ok" },
      ctx: servingCtx,
    });

    expect(result).toEqual({
      blocked: false,
      params: { path: "notes.md", content: "ok" },
    });
  });
});
