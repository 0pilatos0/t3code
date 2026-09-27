import { describe, expect, it } from "vite-plus/test";
import { resolveNewProjectFolder } from "./CreateProjectFolderForm.logic";

function resolve(name: string, platform = "Linux", parentPath = "/projects/") {
  return resolveNewProjectFolder({
    parentPath,
    name,
    platform,
    entries: [{ name: "Existing", fullPath: `${parentPath}Existing` }],
  });
}

describe("resolveNewProjectFolder", () => {
  it.each(["my-project", "my project", "日本語", ".hidden"])(
    "keeps %s inside the selected parent",
    (name) => {
      expect(resolve(name)).toEqual({ path: `/projects/${name}`, error: null });
    },
  );

  it.each([
    ["/projects", "/projects/new"],
    ["/", "/new"],
    ["C:\\projects", "C:\\projects\\new"],
    ["C:\\", "C:\\new"],
    ["\\\\host\\share\\", "\\\\host\\share\\new"],
  ])("joins a folder under %s", (parentPath, path) => {
    expect(resolve(" new ", "Windows", parentPath)).toEqual({ path, error: null });
  });

  it.each(["", " ", ".", "..", "../escape", "a/b", "a\\b", "\0bad"])(
    "rejects %j without producing a creation path",
    (name) => expect(resolve(name).path).toBeNull(),
  );

  it("rejects existing names with the environment's case rules", () => {
    expect(resolve("Existing").path).toBeNull();
    expect(resolve("existing").path).toBe("/projects/existing");
    expect(resolve("existing", "Windows").path).toBeNull();
  });

  it.each(["CON", "nul.txt", "COM1", "LPT9.log", "bad?name", "bad:name", "trailing."])(
    "rejects Windows-only invalid name %s on Windows",
    (name) => {
      expect(resolve(name, "Windows").path).toBeNull();
      expect(resolve(name).path).toBe(`/projects/${name}`);
    },
  );
});
