import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import * as ServerConfig from "../config.ts";

/**
 * Local `opencode serve` processes run in their own process group so T3 can
 * stop the whole group, which also means they outlive a T3 server that is
 * SIGKILLed or crashes. Each spawn is recorded under the state directory and
 * removed on a graceful stop; the next server start stops whatever a dead
 * server left behind.
 */
export interface OpenCodeServerLedgerShape {
  /** Records a spawned server group and returns the effect that forgets it after a graceful stop. */
  readonly track: (server: OpenCodeServerSpawn) => Effect.Effect<Effect.Effect<void>>;
}

interface OpenCodeServerSpawn {
  readonly pid: number;
  readonly port: number;
  /** The argv after the binary, e.g. `["serve", "--hostname=127.0.0.1", "--port=4096"]`. */
  readonly args: ReadonlyArray<string>;
}

export class OpenCodeServerLedger extends Context.Reference<OpenCodeServerLedgerShape>(
  "t3/provider/OpenCodeServerLedger",
  { defaultValue: () => ({ track: () => Effect.succeed(Effect.void) }) },
) {}

const ENTRY_DIRECTORY = "opencode-servers";
const ENTRY_FILE = /^\d+\.json$/;
const STOP_POLL_INTERVAL = "50 millis";
const STOP_POLL_ATTEMPTS = 40;

const ProcessIdentity = Schema.Struct({ pid: Schema.Int, startTime: Schema.String });
type ProcessIdentity = typeof ProcessIdentity.Type;

const OpenCodeServerEntry = Schema.Struct({
  version: Schema.Literal(1),
  pid: Schema.Int,
  pgid: Schema.Int,
  /** Linux `/proc/<pid>/stat` start ticks, or macOS `ps` lstart. Guards against pid reuse. */
  startTime: Schema.String,
  port: Schema.Int,
  args: Schema.Array(Schema.String),
  /** Entries copied along with a state directory are dropped, never acted on. */
  stateDir: Schema.String,
  /** The T3 server that spawned it. Entries of a live owner are never touched. */
  owner: ProcessIdentity,
});
type OpenCodeServerEntry = typeof OpenCodeServerEntry.Type;
const OpenCodeServerEntryJson = Schema.fromJsonString(OpenCodeServerEntry);
const decodeEntry = Schema.decodeUnknownOption(OpenCodeServerEntryJson);
const encodeEntry = Schema.encodeEffect(OpenCodeServerEntryJson);

interface ObservedProcess {
  readonly pgid: number;
  readonly startTime: string;
  readonly command: string;
  readonly zombie: boolean;
}

// `ps -o lstart` prints e.g. `Sat Sep  6 20:51:57 2026`; C locale and UTC keep it stable.
const DARWIN_PS_LINE = /^\s*(\d+)\s+(\S+)\s+(\w{3} \w{3} +\d{1,2} \d{2}:\d{2}:\d{2} \d{4})\s+(.*)$/;

const signalGroup = (pgid: number, signal: NodeJS.Signals) => {
  try {
    process.kill(-pgid, signal);
  } catch {
    // The group may already be gone.
  }
};

const groupExists = (pgid: number) => {
  try {
    process.kill(-pgid, 0);
    return true;
  } catch (cause) {
    return (cause as NodeJS.ErrnoException | undefined)?.code !== "ESRCH";
  }
};

const isOpenCodeServeCommand = (command: string, args: ReadonlyArray<string>) =>
  args[0] === "serve" && command.endsWith(` ${args.join(" ")}`);

/**
 * Builds a ledger for one state directory. `ownerPid` is the T3 server that
 * owns the servers it tracks; it defaults to this process.
 */
export const makeOpenCodeServerLedger = Effect.fn("OpenCodeServerLedger.make")(function* (input: {
  readonly stateDir: string;
  readonly ownerPid?: number;
}) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const platform = yield* HostProcessPlatform;
  const directory = path.join(input.stateDir, ENTRY_DIRECTORY);

  const observeLinux = (pid: number) =>
    Effect.gen(function* () {
      const stat = yield* fs.readFileString(`/proc/${pid}/stat`);
      const cmdline = yield* fs.readFileString(`/proc/${pid}/cmdline`);
      // After the parenthesized comm: state, ppid, pgrp, session, … starttime (index 19).
      const fields = stat
        .slice(stat.lastIndexOf(")") + 2)
        .trim()
        .split(/\s+/);
      const pgid = Number(fields[2]);
      const startTime = fields[19];
      if (!Number.isSafeInteger(pgid) || startTime === undefined) return undefined;
      return {
        pgid,
        startTime,
        command: cmdline.replace(/\0$/, "").replaceAll("\0", " "),
        zombie: fields[0] === "Z",
      } satisfies ObservedProcess;
    });

  const observeDarwin = (pid: number) =>
    spawner
      .string(
        ChildProcess.make(
          "/bin/ps",
          ["-ww", "-o", "pgid=", "-o", "stat=", "-o", "lstart=", "-o", "args=", "-p", String(pid)],
          {
            env: { LC_ALL: "C", TZ: "UTC" },
            extendEnv: true,
            stdin: "ignore",
            stderr: "ignore",
          },
        ),
      )
      .pipe(
        Effect.timeout("5 seconds"),
        Effect.map((output): ObservedProcess | undefined => {
          const match = DARWIN_PS_LINE.exec(output.trim());
          if (match === null) return undefined;
          return {
            pgid: Number(match[1]),
            startTime: match[3]!,
            command: match[4]!,
            zombie: match[2]!.startsWith("Z"),
          };
        }),
      );

  // Windows servers are not detached and are never recorded.
  const observe = (pid: number): Effect.Effect<ObservedProcess | undefined> => {
    if (platform === "linux") return observeLinux(pid).pipe(Effect.orElseSucceed(() => undefined));
    if (platform === "darwin")
      return observeDarwin(pid).pipe(Effect.orElseSucceed(() => undefined));
    return Effect.succeed(undefined);
  };

  const isRunning = (identity: ProcessIdentity) =>
    observe(identity.pid).pipe(
      Effect.map(
        (observed) =>
          observed !== undefined && !observed.zombie && observed.startTime === identity.startTime,
      ),
    );

  const ownerPid = input.ownerPid ?? process.pid;
  const ownerProcess = yield* observe(ownerPid);
  const owner =
    ownerProcess === undefined ? undefined : { pid: ownerPid, startTime: ownerProcess.startTime };

  const track: OpenCodeServerLedgerShape["track"] = (server) =>
    Effect.gen(function* () {
      const observed = yield* observe(server.pid);
      if (owner === undefined || observed === undefined) return Effect.void;
      const entryPath = path.join(directory, `${server.pid}.json`);
      const entry: OpenCodeServerEntry = {
        version: 1,
        pid: server.pid,
        pgid: observed.pgid,
        startTime: observed.startTime,
        port: server.port,
        args: [...server.args],
        stateDir: input.stateDir,
        owner,
      };
      yield* fs.makeDirectory(directory, { recursive: true });
      yield* fs.writeFileString(entryPath, yield* encodeEntry(entry));
      return fs
        .remove(entryPath, { force: true })
        .pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("Could not forget a stopped OpenCode server", { cause }),
          ),
        );
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("Could not record an OpenCode server process", { cause }).pipe(
          Effect.as(Effect.void),
        ),
      ),
    );

  // Signals only a process that still has the recorded pid, start time,
  // process group, and serve argv, so a recycled pid is never touched.
  const stopOrphan = (entry: OpenCodeServerEntry) =>
    Effect.gen(function* () {
      const observed = yield* observe(entry.pid);
      if (
        observed === undefined ||
        observed.zombie ||
        observed.startTime !== entry.startTime ||
        observed.pgid !== entry.pgid ||
        !isOpenCodeServeCommand(observed.command, entry.args)
      ) {
        return;
      }
      yield* Effect.logInfo("Stopping an OpenCode server left by a previous T3 Code server", {
        pid: entry.pid,
        port: entry.port,
      });
      signalGroup(entry.pgid, "SIGTERM");
      for (let attempt = 0; attempt < STOP_POLL_ATTEMPTS && groupExists(entry.pgid); attempt++) {
        yield* Effect.sleep(STOP_POLL_INTERVAL);
      }
      // The group never emptied, so its pgid cannot have been reused.
      if (groupExists(entry.pgid)) signalGroup(entry.pgid, "SIGKILL");
    });

  const reapEntry = (entryPath: string) =>
    Effect.gen(function* () {
      const entry = decodeEntry(yield* fs.readFileString(entryPath));
      if (Option.isSome(entry) && entry.value.stateDir === input.stateDir) {
        if (yield* isRunning(entry.value.owner)) return;
        yield* stopOrphan(entry.value);
      }
      yield* fs.remove(entryPath, { force: true });
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("Could not clean up a recorded OpenCode server", { entryPath, cause }),
      ),
    );

  /** Stops recorded servers whose owning T3 server is gone and drops stale entries. */
  const reapOrphans = Effect.gen(function* () {
    const names = yield* fs.readDirectory(directory).pipe(Effect.orElseSucceed(() => []));
    yield* Effect.forEach(
      names.filter((name) => ENTRY_FILE.test(name)),
      (name) => reapEntry(path.join(directory, name)),
      { concurrency: "unbounded", discard: true },
    );
  });

  return { track, reapOrphans };
});

export const layer = Layer.effect(
  OpenCodeServerLedger,
  Effect.gen(function* () {
    const config = yield* ServerConfig.ServerConfig;
    const ledger = yield* makeOpenCodeServerLedger({ stateDir: config.stateDir });
    // Reaping waits for orphans to exit, so it must not hold up startup.
    yield* ledger.reapOrphans.pipe(Effect.forkScoped);
    return OpenCodeServerLedger.of({ track: ledger.track });
  }),
);
