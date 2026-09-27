import type { FilesystemBrowseEntry } from "@t3tools/contracts";
import { ensureBrowseDirectoryPath } from "../lib/projectPaths";
import { isWindowsPlatform } from "../lib/utils";

/** Restricts the name prompt to one new child of the directory being browsed. */
export function resolveNewProjectFolder(input: {
  readonly parentPath: string;
  readonly name: string;
  readonly platform: string;
  readonly entries: ReadonlyArray<FilesystemBrowseEntry>;
}): { path: string; error: null } | { path: null; error: string } {
  const name = input.name.trim();
  if (!name) return { path: null, error: "Enter a folder name." };
  if (name === "." || name === ".." || /[\\/]/.test(name) || name.includes("\0")) {
    return { path: null, error: "Enter a folder name, not a path." };
  }
  const windows = isWindowsPlatform(input.platform);
  if (
    windows &&
    (/[<>:"|?*]/.test(name) ||
      Array.from(name).some((character) => character.charCodeAt(0) < 32) ||
      name.endsWith(".") ||
      /^(con|prn|aux|nul|com[1-9¹²³]|lpt[1-9¹²³])(?:\.|$)/i.test(name))
  ) {
    return { path: null, error: "This folder name is not valid on Windows." };
  }
  // This listing is only an early hint. The existing create-or-add operation
  // can reuse a directory created since browsing or resolved by the filesystem.
  if (
    input.entries.some((entry) =>
      windows ? entry.name.toLowerCase() === name.toLowerCase() : entry.name === name,
    )
  ) {
    return { path: null, error: "A folder with this name already exists." };
  }
  return { path: `${ensureBrowseDirectoryPath(input.parentPath)}${name}`, error: null };
}
