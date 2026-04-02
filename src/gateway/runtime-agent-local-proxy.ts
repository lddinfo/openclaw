import { buildTimeoutAbortSignal } from "../utils/fetch-timeout.js";

export function readRuntimeAgentLocalProxyBaseUrl(): string {
  const raw =
    process.env.RUNTIME_AGENT_LOCAL_PROXY_BASE_URL?.trim() ||
    `http://127.0.0.1:${process.env.RUNTIME_AGENT_LOCAL_PROXY_PORT?.trim() || "15662"}`;
  return raw.replace(/\/+$/, "");
}

function readRuntimeAgentLocalProxyToken(): string | undefined {
  const token =
    process.env.RUNTIME_AGENT_LOCAL_PROXY_TOKEN?.trim() ||
    process.env.OPENCLAW_BRIDGE_TOKEN?.trim();
  return token || undefined;
}

export async function proxyExternalHttpViaRuntimeAgent(params: {
  url: string;
  method?: "GET" | "POST" | "PUT" | "PATCH" | "DELETE";
  headers?: Record<string, string>;
  body?: string;
  timeoutMs?: number;
}): Promise<Response> {
  const proxyUrl = `${readRuntimeAgentLocalProxyBaseUrl()}/proxy/http`;
  const token = readRuntimeAgentLocalProxyToken();
  const { signal, cleanup } = buildTimeoutAbortSignal({
    timeoutMs: params.timeoutMs,
  });
  try {
    return await fetch(proxyUrl, {
      method: "POST",
      headers: {
        accept: "application/json",
        "content-type": "application/json",
        ...(token ? { "x-runtime-agent-local-token": token } : {}),
      },
      body: JSON.stringify({
        url: params.url,
        method: params.method ?? "GET",
        headers: params.headers ?? {},
        body: params.body,
        timeoutMs: params.timeoutMs,
      }),
      signal,
    });
  } finally {
    cleanup();
  }
}
