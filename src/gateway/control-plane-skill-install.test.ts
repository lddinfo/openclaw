import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import JSZip from "jszip";
import { describe, expect, it } from "vitest";
import { installSkillPackageFromInlineArchive } from "./control-plane-skill-install.js";

async function withTempDir<T>(run: (dir: string) => Promise<T>): Promise<T> {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-skill-install-"));
  try {
    return await run(dir);
  } finally {
    await fs.rm(dir, { recursive: true, force: true });
  }
}

describe("control-plane skill install", () => {
  it("normalizes nested zip wrappers and strips __MACOSX metadata", async () => {
    await withTempDir(async (workspaceDir) => {
      const zip = new JSZip();
      zip.file(
        "find-skills-safe/find-skills-safe/SKILL.md",
        [
          "---",
          "name: find-skills-safe",
          "description: test skill",
          "---",
          "",
          "# find-skills-safe",
          "",
          "test",
        ].join("\n"),
      );
      zip.file("find-skills-safe/__MACOSX/._SKILL.md", "metadata");
      zip.file("__MACOSX/._find-skills-safe", "metadata");
      zip.file("find-skills-safe/.DS_Store", "ignored");

      const archiveBase64 = await zip.generateAsync({ type: "base64" });
      const result = await installSkillPackageFromInlineArchive({
        workspaceDir,
        archiveBase64,
        archiveFileName: "find-skills-safe.zip",
        skillKey: "find-skills-safe",
        artifactFormat: "zip",
      });

      expect(result.ok).toBe(true);
      if (!result.ok) {
        return;
      }

      const skillRoot = path.join(workspaceDir, "skills", "find-skills-safe");
      await expect(fs.access(path.join(skillRoot, "SKILL.md"))).resolves.toBeUndefined();
      await expect(fs.access(path.join(skillRoot, "__MACOSX"))).rejects.toBeTruthy();
      await expect(fs.access(path.join(skillRoot, "find-skills-safe"))).rejects.toBeTruthy();
    });
  });
});
