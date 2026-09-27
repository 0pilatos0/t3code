# Threads from the command line

The `t3 thread` commands let a script, or an agent working in another thread,
start threads, send them messages, and read the replies. They talk to the T3 Code
running on the same machine: the desktop app or a `t3` server. Install the `t3`
CLI first ([Install T3 Code](./install.md#command-line)).

## Start a thread

```sh
t3 thread start ~/code/app "Find why the tests are slow"
```

This prints the new thread's id. The thread gets the same defaults as a new thread
in the app: the project's model and permission mode, **New worktree** or local
mode, the base branch the project folder is on, and the project's setup script.
The project can be its id or any path inside it, so `.` works from inside the
project or one of its worktrees.

| Flag                      | Effect                                           |
| ------------------------- | ------------------------------------------------ |
| `--model <slug>`          | Use this model, like `claude-sonnet-5`           |
| `--provider <instance>`   | Pick the provider, like `codex` or `claudeAgent` |
| `--worktree` or `--local` | Override the project's default                   |
| `--base <branch>`         | Start the new worktree from this branch          |

Without `--model`, a thread uses the project's default model, else the model of
the latest thread.

## Send a message and wait for the reply

```sh
t3 thread send <thread-id> "Now fix it" --wait
```

If the agent is working, `send` waits for its turn to end first, so it never
interrupts the agent. With `--wait`, `start` and `send` wait for the turn to end
and print the agent's final reply. The exit code tells a script what happened:

| Exit code | Meaning                                                        |
| --------- | -------------------------------------------------------------- |
| 0         | The turn ended and the reply was printed                       |
| 1         | The turn failed or was interrupted                             |
| 2         | The agent is waiting for your approval or an answer in the app |

## Find and read threads

`t3 thread list` shows threads, most recent first. Pass `--project <path>` to see
one project. `t3 thread show <thread-id>` prints the latest turn, and `--turns 3`
prints more. In the app, a thread's id is under **Copy > Thread ID** in its menu.

Every command takes `--json` for scripts. `start` and `send` then print the
thread's `branch` and `worktreePath` too.
