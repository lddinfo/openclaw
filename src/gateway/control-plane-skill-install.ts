// AGENT_BOT_COMPAT: install a skill-registry artifact (base install-ticket downloadUrl) into an agent workspace.

import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";
import { extractArchive } from "../agents/skills-install-extract.js";
import { isWithinDir } from "../infra/path-safety.js";
import { ensureDir } from "../utils.js";
import { proxyExternalHttpViaRuntimeAgent } from "./runtime-agent-local-proxy.js";

const SAFE_SKILL_KEY = /^[a-z0-9][a-z0-9-_]*$/;

function isNodeReadableStream(value: unknown): value is NodeJS.ReadableStream {
  return Boolean(value && typeof (value as NodeJS.ReadableStream).pipe === "function");
}

async function hashFileSha256(filePath: string): Promise<string> {
  const hash = createHash("sha256");
  const stream = fs.createReadStream(filePath);
  return await new Promise<string>((resolve, reject) => {
    stream.on("data", (chunk) => {
      hash.update(chunk as Buffer);
    });
    stream.on("error", reject);
    stream.on("end", () => {
      resolve(hash.digest("hex"));
    });
  });
}

async function fileExists(filePath: string): Promise<boolean> {
  try {
    await fs.promises.access(filePath);
    return true;
  } catch {
    return false;
  }
}

function resolveArchiveTypeAndSuffix(params: {
  artifactFormat?: string;
  downloadUrl: string;
}): { archiveType: "zip" | "tar.gz"; suffix: string } | undefined {
  const fmt = params.artifactFormat?.trim().toLowerCase();
  if (fmt === "zip") {
    return { archiveType: "zip", suffix: ".zip" };
  }
  if (fmt === "tar_gz" || fmt === "tar.gz" || fmt === "tgz") {
    return { archiveType: "tar.gz", suffix: ".tar.gz" };
  }
  try {
    const p = new URL(params.downloadUrl).pathname.toLowerCase();
    if (p.endsWith(".zip")) {
      return { archiveType: "zip", suffix: ".zip" };
    }
    if (p.endsWith(".tar.gz") || p.endsWith(".tgz")) {
      return { archiveType: "tar.gz", suffix: ".tar.gz" };
    }
  } catch {
    /* ignore */
  }
  return undefined;
}

/**
 * When the registry archive uses `single-folder` layout (`my-skill/SKILL.md`), extraction leaves
 * `skills/<skillKey>/my-skill/SKILL.md`. Promote inner files to `skills/<skillKey>/` when safe.
 */
function isMacOsMetadataEntry(name: string): boolean {
  return name === "__MACOSX" || name === ".DS_Store" || name.startsWith("._");
}

async function removeMacOsMetadataEntries(rootDir: string): Promise<void> {
  let entries: fs.Dirent[];
  try {
    entries = await fs.promises.readdir(rootDir, { withFileTypes: true });
  } catch {
    return;
  }

  for (const entry of entries) {
    const entryPath = path.join(rootDir, entry.name);
    if (isMacOsMetadataEntry(entry.name)) {
      await fs.promises.rm(entryPath, { recursive: true, force: true }).catch(() => undefined);
      continue;
    }
    if (entry.isDirectory()) {
      await removeMacOsMetadataEntries(entryPath);
    }
  }
}

async function moveDirectoryContentsToSkillRoot(
  sourceDir: string,
  skillRoot: string,
): Promise<boolean> {
  const entries = await fs.promises.readdir(sourceDir, { withFileTypes: true }).catch(() => []);
  for (const entry of entries) {
    const sourcePath = path.join(sourceDir, entry.name);
    if (isMacOsMetadataEntry(entry.name)) {
      await fs.promises.rm(sourcePath, { recursive: true, force: true }).catch(() => undefined);
      continue;
    }

    const targetPath = path.join(skillRoot, entry.name);
    if (entry.isDirectory() && path.resolve(targetPath) === path.resolve(sourceDir)) {
      const nestedMoved = await moveDirectoryContentsToSkillRoot(sourcePath, skillRoot);
      if (!nestedMoved) {
        return false;
      }
      await fs.promises.rm(sourcePath, { recursive: true, force: true });
      continue;
    }

    if (await fileExists(targetPath)) {
      return false;
    }
    await fs.promises.rename(sourcePath, targetPath);
  }
  return true;
}

async function promoteSingleFolderSkillLayout(skillRoot: string): Promise<void> {
  await removeMacOsMetadataEntries(skillRoot);

  for (let depth = 0; depth < 8; depth += 1) {
    const rootSkillMd = path.join(skillRoot, "SKILL.md");
    if (await fileExists(rootSkillMd)) {
      return;
    }

    let entries: fs.Dirent[];
    try {
      entries = await fs.promises.readdir(skillRoot, { withFileTypes: true });
    } catch {
      return;
    }

    const effectiveEntries = entries.filter((entry) => !isMacOsMetadataEntry(entry.name));
    const dirs = effectiveEntries.filter((entry) => entry.isDirectory());
    const files = effectiveEntries.filter((entry) => entry.isFile());
    if (dirs.length !== 1 || files.length > 0) {
      return;
    }

    const inner = path.join(skillRoot, dirs[0].name);
    const moved = await moveDirectoryContentsToSkillRoot(inner, skillRoot);
    if (!moved) {
      return;
    }
    await fs.promises.rm(inner, { recursive: true, force: true });
    await removeMacOsMetadataEntries(skillRoot);
  }
}

export type ControlPlaneRegistryInstallOk = {
  ok: true;
  skillKey: string;
  installedPath: string;
  bytes: number;
  sha256?: string;
};

export type ControlPlaneRegistryInstallErr = {
  ok: false;
  message: string;
};

export type ControlPlaneRegistryInstallResult =
  | ControlPlaneRegistryInstallOk
  | ControlPlaneRegistryInstallErr;

type InstallPreparedArchiveParams = {
  workspaceDir: string;
  archivePath: string;
  skillKey: string;
  artifactFormat?: string;
  expectedSha256?: string;
  stripComponents?: number;
  timeoutMs?: number;
};

async function installSkillPackageFromPreparedArchive(
  params: InstallPreparedArchiveParams,
): Promise<ControlPlaneRegistryInstallResult> {
  const skillKey = params.skillKey.trim();
  if (!SAFE_SKILL_KEY.test(skillKey)) {
    return { ok: false, message: "invalid skillKey" };
  }

  const workspaceResolved = path.resolve(params.workspaceDir);
  const skillsRoot = path.join(workspaceResolved, "skills");
  const skillRoot = path.join(skillsRoot, skillKey);

  if (!isWithinDir(workspaceResolved, skillRoot)) {
    return { ok: false, message: "refusing skill install path outside workspace" };
  }

  const resolved = resolveArchiveTypeAndSuffix({
    artifactFormat: params.artifactFormat,
    downloadUrl: params.archivePath,
  });
  if (!resolved) {
    return {
      ok: false,
      message: "could not determine archive type; pass artifactFormat zip or tar_gz",
    };
  }

  const timeoutMs =
    typeof params.timeoutMs === "number" &&
    Number.isFinite(params.timeoutMs) &&
    params.timeoutMs > 0
      ? Math.floor(params.timeoutMs)
      : 120_000;

  const strip =
    typeof params.stripComponents === "number" && Number.isFinite(params.stripComponents)
      ? Math.max(0, Math.floor(params.stripComponents))
      : 0;

  try {
    await ensureDir(skillsRoot);

    const bytes = (await fs.promises.stat(params.archivePath)).size;
    const sha256 = await hashFileSha256(params.archivePath);
    const expected = params.expectedSha256?.trim().toLowerCase();
    if (expected && sha256 !== expected) {
      return { ok: false, message: "sha256 mismatch for uploaded artifact" };
    }

    await fs.promises.rm(skillRoot, { recursive: true, force: true });
    await ensureDir(skillRoot);

    const extractResult = await extractArchive({
      archivePath: params.archivePath,
      archiveType: resolved.archiveType,
      targetDir: skillRoot,
      stripComponents: strip,
      timeoutMs,
    });
    if (extractResult.code !== 0) {
      const detail = [extractResult.stderr, extractResult.stdout].filter(Boolean).join("\n").trim();
      return {
        ok: false,
        message: detail || "archive extraction failed",
      };
    }

    await promoteSingleFolderSkillLayout(skillRoot);

    if (!(await fileExists(path.join(skillRoot, "SKILL.md")))) {
      return {
        ok: false,
        message:
          "installed archive does not contain SKILL.md at skill root (after layout normalize)",
      };
    }

    return {
      ok: true,
      skillKey,
      installedPath: skillRoot,
      bytes,
      sha256,
    };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return { ok: false, message };
  }
}

export async function installSkillPackageFromRegistryDownload(params: {
  workspaceDir: string;
  downloadUrl: string;
  skillKey: string;
  artifactFormat?: string;
  expectedSha256?: string;
  stripComponents?: number;
  timeoutMs?: number;
}): Promise<ControlPlaneRegistryInstallResult> {
  const resolved = resolveArchiveTypeAndSuffix({
    artifactFormat: params.artifactFormat,
    downloadUrl: params.downloadUrl.trim(),
  });
  if (!resolved) {
    return {
      ok: false,
      message: "could not determine archive type; pass artifactFormat zip or tar_gz",
    };
  }

  const timeoutMs =
    typeof params.timeoutMs === "number" &&
    Number.isFinite(params.timeoutMs) &&
    params.timeoutMs > 0
      ? Math.floor(params.timeoutMs)
      : 120_000;

  const strip =
    typeof params.stripComponents === "number" && Number.isFinite(params.stripComponents)
      ? Math.max(0, Math.floor(params.stripComponents))
      : 0;

  const workspaceResolved = path.resolve(params.workspaceDir);
  const skillsRoot = path.join(workspaceResolved, "skills");
  const tempPath = path.join(skillsRoot, `.registry-staging-${randomUUID()}${resolved.suffix}`);

  try {
    await ensureDir(skillsRoot);
    const response = await proxyExternalHttpViaRuntimeAgent({
      url: params.downloadUrl.trim(),
      method: "GET",
      timeoutMs,
    });
    if (!response.ok || !response.body) {
      return {
        ok: false,
        message: `download failed (${response.status} ${response.statusText})`,
      };
    }
    const body = response.body as unknown;
    const readable = isNodeReadableStream(body)
      ? body
      : Readable.fromWeb(body as NodeReadableStream);
    const file = fs.createWriteStream(tempPath);
    await pipeline(readable, file);

    return await installSkillPackageFromPreparedArchive({
      workspaceDir: params.workspaceDir,
      archivePath: tempPath,
      skillKey: params.skillKey,
      artifactFormat: params.artifactFormat,
      expectedSha256: params.expectedSha256,
      stripComponents: strip,
      timeoutMs,
    });
  } finally {
    await fs.promises.rm(tempPath, { force: true }).catch(() => undefined);
  }
}

export async function installSkillPackageFromInlineArchive(params: {
  workspaceDir: string;
  archiveBase64: string;
  archiveFileName?: string;
  skillKey: string;
  artifactFormat?: string;
  expectedSha256?: string;
  stripComponents?: number;
  timeoutMs?: number;
}): Promise<ControlPlaneRegistryInstallResult> {
  const resolved = resolveArchiveTypeAndSuffix({
    artifactFormat: params.artifactFormat,
    downloadUrl: params.archiveFileName?.trim() || "",
  });
  if (!resolved) {
    return {
      ok: false,
      message: "could not determine archive type for uploaded skill package",
    };
  }

  const workspaceResolved = path.resolve(params.workspaceDir);
  const skillsRoot = path.join(workspaceResolved, "skills");
  const tempPath = path.join(skillsRoot, `.registry-inline-${randomUUID()}${resolved.suffix}`);

  try {
    await ensureDir(skillsRoot);
    const archiveBuffer = Buffer.from(params.archiveBase64.trim(), "base64");
    if (archiveBuffer.length === 0) {
      return { ok: false, message: "uploaded archive is empty" };
    }
    await fs.promises.writeFile(tempPath, archiveBuffer);

    return await installSkillPackageFromPreparedArchive({
      workspaceDir: params.workspaceDir,
      archivePath: tempPath,
      skillKey: params.skillKey,
      artifactFormat: params.artifactFormat,
      expectedSha256: params.expectedSha256,
      stripComponents: params.stripComponents,
      timeoutMs: params.timeoutMs,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return { ok: false, message };
  } finally {
    await fs.promises.rm(tempPath, { force: true }).catch(() => undefined);
  }
}
