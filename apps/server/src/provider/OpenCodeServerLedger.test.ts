// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import { makeOpenCodeServerLedger } from "./OpenCodeServerLedger.ts";

const SERVE_ARGS = ["serve", "--hostname=127.0.0.1", "--port=4096"];

const groupExists = (pgid: number) => {
  try {
    process.kill(-pgid, 0);
    return true;
  } catch {
    return false;
  }
};

/**
 * Spawns a throwaway process group shaped like `opencode serve`: a shell
 * leader whose argv ends in the serve arguments, plus a `sleep` member.
 */
const spawnGroup = (args: ReadonlyArray<string>) =>
  Effect.acquireRelease(
    Effect.gen(function* () {
      const child = NodeChildProcess.spawn("/bin/sh", ["-c", "sleep 600 & wait", ...args], {
        detached: true,
        stdio: "ignore",
      });
      const exited = yield* Deferred.make<NodeJS.Signals | null>();
      child.once("exit", (_code, signal) => Deferred.doneUnsafe(exited, Effect.succeed(signal)));
      const pid = child.pid;
      if (pid === undefined) return yield* Effect.die("spawn failed");
      return { pid, exited: Deferred.await(exited) };
    }),
    ({ pid }) =>
      Effect.sync(() => {
        if (groupExists(pid)) process.kill(-pid, "SIGKILL");
      }),
  );

/** A T3 server that recorded its OpenCode server and then died without cleanup. */
const recordFromDeadServer = (stateDir: string, server: { readonly pid: number }) =>
  Effect.gen(function* () {
    const previousServer = yield* spawnGroup([]);
    const previousLedger = yield* makeOpenCodeServerLedger({
      stateDir,
      ownerPid: previousServer.pid,
    });
    // The previous server never gets to forget its entry.
    yield* Effect.asVoid(previousLedger.track({ pid: server.pid, port: 4096, args: SERVE_ARGS }));
    process.kill(-previousServer.pid, "SIGKILL");
    yield* previousServer.exited;
  });

const hostPlatform = HostProcessPlatform.defaultValue();
// procps accepts the same `ps` flags as macOS, so Linux also covers the macOS path.
const observedPlatforms: ReadonlyArray<NodeJS.Platform> =
  hostPlatform === "linux" ? ["linux", "darwin"] : hostPlatform === "darwin" ? ["darwin"] : [];

describe.each(observedPlatforms)("OpenCodeServerLedger observing as %s", (platform) => {
  const provideHost = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.provideService(HostProcessPlatform, platform),
      Effect.provide(NodeServices.layer),
    );

  it.live("stops an OpenCode server group left by a server that died", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const stateDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-opencode-ledger-" });
      const orphan = yield* spawnGroup(SERVE_ARGS);
      yield* recordFromDeadServer(stateDir, orphan);
      expect(yield* fs.readDirectory(path.join(stateDir, "opencode-servers"))).toHaveLength(1);

      const restarted = yield* makeOpenCodeServerLedger({ stateDir });
      yield* restarted.reapOrphans;

      expect(yield* orphan.exited).toBe("SIGTERM");
      expect(groupExists(orphan.pid)).toBe(false);
      expect(yield* fs.readDirectory(path.join(stateDir, "opencode-servers"))).toEqual([]);
    }).pipe(provideHost),
  );

  it.live("leaves a recycled pid alone when its start time does not match", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const stateDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-opencode-ledger-" });
      const unrelated = yield* spawnGroup(SERVE_ARGS);
      yield* recordFromDeadServer(stateDir, unrelated);
      const entryPath = path.join(stateDir, "opencode-servers", `${unrelated.pid}.json`);
      // Same pid, a different process: what a recycled pid looks like.
      const entry = yield* fs.readFileString(entryPath);
      yield* fs.writeFileString(
        entryPath,
        entry.replace(/"startTime":"[^"]+","port"/, '"startTime":"1","port"'),
      );

      const restarted = yield* makeOpenCodeServerLedger({ stateDir });
      yield* restarted.reapOrphans;

      expect(groupExists(unrelated.pid)).toBe(true);
      expect(yield* fs.exists(entryPath)).toBe(false);
    }).pipe(provideHost),
  );

  it.live("leaves the servers of a running T3 server alone", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const stateDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-opencode-ledger-" });
      const running = yield* makeOpenCodeServerLedger({ stateDir });
      const server = yield* spawnGroup(SERVE_ARGS);
      const forget = yield* running.track({ pid: server.pid, port: 4096, args: SERVE_ARGS });

      const other = yield* makeOpenCodeServerLedger({ stateDir });
      yield* other.reapOrphans;
      expect(groupExists(server.pid)).toBe(true);

      yield* forget;
      expect(yield* fs.readDirectory(path.join(stateDir, "opencode-servers"))).toEqual([]);
    }).pipe(provideHost),
  );
});
