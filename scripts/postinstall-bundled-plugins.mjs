#!/usr/bin/env node
// Runs after install to restore bundled extension runtime deps.
// Installed builds can lazy-load bundled plugin code through root dist chunks,
// so runtime dependencies declared in dist/extensions/*/package.json must also
// resolve from the package root node_modules. Source checkouts resolve bundled
// plugin deps from the workspace root, so stale plugin-local node_modules must
// not linger under extensions/* and shadow the root graph.
import { spawnSync } from "node:child_process";
import { cpSync, existsSync, mkdirSync, readdirSync, readFileSync, rmSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { resolveNpmRunner } from "./npm-runner.mjs";

export const BUNDLED_PLUGIN_INSTALL_TARGETS = [];

const __dirname = dirname(fileURLToPath(import.meta.url));
const DEFAULT_EXTENSIONS_DIR = join(__dirname, "..", "dist", "extensions");
const DEFAULT_PACKAGE_ROOT = join(__dirname, "..");
const DISABLE_POSTINSTALL_ENV = "OPENCLAW_DISABLE_BUNDLED_PLUGIN_POSTINSTALL";

function readJson(filePath) {
  return JSON.parse(readFileSync(filePath, "utf8"));
}

function dependencySentinelPath(depName) {
  return join("node_modules", ...depName.split("/"), "package.json");
}

function dependencyNodeModulesPath(nodeModulesDir, depName) {
  return join(nodeModulesDir, ...depName.split("/"));
}

function collectRuntimeDeps(packageJson) {
  return {
    ...packageJson.dependencies,
    ...packageJson.optionalDependencies,
  };
}

function collectInstalledRuntimeClosure(params) {
  const nodeModulesDir = params.nodeModulesDir;
  const dependencySpecs = params.dependencySpecs;
  const pathExists = params.existsSync ?? existsSync;
  const readJsonFile = params.readJson ?? readJson;
  const packageCache = new Map();
  const closure = new Set();
  const queue = Object.entries(dependencySpecs);

  while (queue.length > 0) {
    const [depName] = queue.shift();
    if (closure.has(depName)) {
      continue;
    }

    const packageJsonPath = join(
      dependencyNodeModulesPath(nodeModulesDir, depName),
      "package.json",
    );
    if (!pathExists(packageJsonPath)) {
      return null;
    }

    const packageJson = packageCache.get(depName) ?? readJsonFile(packageJsonPath);
    packageCache.set(depName, packageJson);
    closure.add(depName);

    for (const [childName, childSpec] of Object.entries(collectRuntimeDeps(packageJson))) {
      queue.push([childName, childSpec]);
    }
  }

  return [...closure].toSorted((a, b) => a.localeCompare(b));
}

function restoreBundledPluginRuntimeDepsFromPayload(params) {
  const extensionsDir = params.extensionsDir ?? DEFAULT_EXTENSIONS_DIR;
  const packageRoot = params.packageRoot ?? DEFAULT_PACKAGE_ROOT;
  const pathExists = params.existsSync ?? existsSync;
  const readDir = params.readdirSync ?? readdirSync;
  const readJsonFile = params.readJson ?? readJson;
  const restored = new Set();

  if (!pathExists(extensionsDir)) {
    return [];
  }

  for (const entry of readDir(extensionsDir, { withFileTypes: true })) {
    if (!entry.isDirectory()) {
      continue;
    }

    const pluginDir = join(extensionsDir, entry.name);
    const packageJsonPath = join(pluginDir, "package.json");
    const stagedNodeModulesDir = join(pluginDir, "node_modules");
    if (!pathExists(packageJsonPath) || !pathExists(stagedNodeModulesDir)) {
      continue;
    }

    let packageJson;
    try {
      packageJson = readJsonFile(packageJsonPath);
    } catch {
      continue;
    }

    const dependencySpecs = collectRuntimeDeps(packageJson);
    if (Object.keys(dependencySpecs).length === 0) {
      continue;
    }

    const dependencyNames = collectInstalledRuntimeClosure({
      nodeModulesDir: stagedNodeModulesDir,
      dependencySpecs,
      existsSync: pathExists,
      readJson: readJsonFile,
    });
    if (dependencyNames === null) {
      continue;
    }

    for (const depName of dependencyNames) {
      const sentinelPath = join(packageRoot, dependencySentinelPath(depName));
      if (pathExists(sentinelPath)) {
        continue;
      }
      const sourcePath = dependencyNodeModulesPath(stagedNodeModulesDir, depName);
      if (!pathExists(sourcePath)) {
        continue;
      }
      const targetPath = join(packageRoot, "node_modules", ...depName.split("/"));
      mkdirSync(dirname(targetPath), { recursive: true });
      cpSync(sourcePath, targetPath, { recursive: true, force: true, dereference: true });
      restored.add(depName);
    }
  }

  return [...restored].toSorted((a, b) => a.localeCompare(b));
}

export function discoverBundledPluginRuntimeDeps(params = {}) {
  const extensionsDir = params.extensionsDir ?? DEFAULT_EXTENSIONS_DIR;
  const pathExists = params.existsSync ?? existsSync;
  const readDir = params.readdirSync ?? readdirSync;
  const readJsonFile = params.readJson ?? readJson;
  const deps = new Map(
    BUNDLED_PLUGIN_INSTALL_TARGETS.map((target) => [
      target.name,
      {
        name: target.name,
        version: target.version,
        sentinelPath: dependencySentinelPath(target.name),
        pluginIds: [...(target.pluginIds ?? [])],
      },
    ]),
  );

  if (!pathExists(extensionsDir)) {
    return [...deps.values()].toSorted((a, b) => a.name.localeCompare(b.name));
  }

  for (const entry of readDir(extensionsDir, { withFileTypes: true })) {
    if (!entry.isDirectory()) {
      continue;
    }
    const pluginId = entry.name;
    const packageJsonPath = join(extensionsDir, pluginId, "package.json");
    if (!pathExists(packageJsonPath)) {
      continue;
    }
    try {
      const packageJson = readJsonFile(packageJsonPath);
      for (const [name, version] of Object.entries(collectRuntimeDeps(packageJson))) {
        const existing = deps.get(name);
        if (existing) {
          if (existing.version !== version) {
            continue;
          }
          if (!existing.pluginIds.includes(pluginId)) {
            existing.pluginIds.push(pluginId);
          }
          continue;
        }
        deps.set(name, {
          name,
          version,
          sentinelPath: dependencySentinelPath(name),
          pluginIds: [pluginId],
        });
      }
    } catch {
      // Ignore malformed plugin manifests; runtime will surface those separately.
    }
  }

  return [...deps.values()]
    .map((dep) => ({
      ...dep,
      pluginIds: [...dep.pluginIds].toSorted((a, b) => a.localeCompare(b)),
    }))
    .toSorted((a, b) => a.name.localeCompare(b.name));
}

export function createNestedNpmInstallEnv(env = process.env) {
  const nextEnv = { ...env };
  delete nextEnv.npm_config_global;
  delete nextEnv.npm_config_location;
  delete nextEnv.npm_config_prefix;
  return nextEnv;
}

export function isSourceCheckoutRoot(params) {
  const pathExists = params.existsSync ?? existsSync;
  return (
    pathExists(join(params.packageRoot, ".git")) &&
    pathExists(join(params.packageRoot, "src")) &&
    pathExists(join(params.packageRoot, "extensions"))
  );
}

export function pruneBundledPluginSourceNodeModules(params = {}) {
  const extensionsDir = params.extensionsDir ?? join(DEFAULT_PACKAGE_ROOT, "extensions");
  const pathExists = params.existsSync ?? existsSync;
  const readDir = params.readdirSync ?? readdirSync;
  const removePath = params.rmSync ?? rmSync;

  if (!pathExists(extensionsDir)) {
    return;
  }

  for (const entry of readDir(extensionsDir, { withFileTypes: true })) {
    if (!entry.isDirectory() || entry.isSymbolicLink()) {
      continue;
    }

    const pluginDir = join(extensionsDir, entry.name);
    if (!pathExists(join(pluginDir, "package.json"))) {
      continue;
    }

    removePath(join(pluginDir, "node_modules"), { recursive: true, force: true });
  }
}

function shouldRunBundledPluginPostinstall(params) {
  if (params.env?.[DISABLE_POSTINSTALL_ENV]?.trim()) {
    return false;
  }
  if (!params.existsSync(params.extensionsDir)) {
    return false;
  }
  return true;
}

export function runBundledPluginPostinstall(params = {}) {
  const env = params.env ?? process.env;
  const extensionsDir = params.extensionsDir ?? DEFAULT_EXTENSIONS_DIR;
  const packageRoot = params.packageRoot ?? DEFAULT_PACKAGE_ROOT;
  const spawn = params.spawnSync ?? spawnSync;
  const pathExists = params.existsSync ?? existsSync;
  const log = params.log ?? console;
  if (env?.[DISABLE_POSTINSTALL_ENV]?.trim()) {
    return;
  }
  if (isSourceCheckoutRoot({ packageRoot, existsSync: pathExists })) {
    try {
      pruneBundledPluginSourceNodeModules({
        extensionsDir: join(packageRoot, "extensions"),
        existsSync: pathExists,
        readdirSync: params.readdirSync,
        rmSync: params.rmSync,
      });
    } catch (e) {
      log.warn(`[postinstall] could not prune bundled plugin source node_modules: ${String(e)}`);
    }
    return;
  }
  if (
    !shouldRunBundledPluginPostinstall({
      env,
      extensionsDir,
      packageRoot,
      existsSync: pathExists,
    })
  ) {
    return;
  }
  const restoredSpecs = restoreBundledPluginRuntimeDepsFromPayload({
    extensionsDir,
    packageRoot,
    existsSync: pathExists,
  });
  if (restoredSpecs.length > 0) {
    log.log(
      `[postinstall] restored bundled plugin deps from packaged payload: ${restoredSpecs.join(", ")}`,
    );
  }
  const runtimeDeps =
    params.runtimeDeps ??
    discoverBundledPluginRuntimeDeps({ extensionsDir, existsSync: pathExists });
  const missingSpecs = runtimeDeps
    .filter((dep) => !pathExists(join(packageRoot, dep.sentinelPath)))
    .map((dep) => `${dep.name}@${dep.version}`);

  if (missingSpecs.length === 0) {
    return;
  }

  try {
    const nestedEnv = createNestedNpmInstallEnv(env);
    const npmRunner =
      params.npmRunner ??
      resolveNpmRunner({
        env: nestedEnv,
        execPath: params.execPath,
        existsSync: pathExists,
        platform: params.platform,
        comSpec: params.comSpec,
        npmArgs: ["install", "--omit=dev", "--no-save", "--package-lock=false", ...missingSpecs],
      });
    const result = spawn(npmRunner.command, npmRunner.args, {
      cwd: packageRoot,
      encoding: "utf8",
      env: npmRunner.env ?? nestedEnv,
      stdio: "pipe",
      shell: npmRunner.shell,
      windowsVerbatimArguments: npmRunner.windowsVerbatimArguments,
    });
    if (result.status !== 0) {
      const output = [result.stderr, result.stdout].filter(Boolean).join("\n").trim();
      throw new Error(output || "npm install failed");
    }
    log.log(`[postinstall] installed bundled plugin deps: ${missingSpecs.join(", ")}`);
  } catch (e) {
    // Non-fatal: gateway will surface the missing dep via doctor.
    log.warn(`[postinstall] could not install bundled plugin deps: ${String(e)}`);
  }
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? "").href) {
  runBundledPluginPostinstall();
}
