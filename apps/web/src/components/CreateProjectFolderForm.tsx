import { type FilesystemBrowseEntry } from "@t3tools/contracts";
import { useId, useLayoutEffect, useRef, useState } from "react";
import { Button } from "./ui/button";
import { Input } from "./ui/input";
import { Label } from "./ui/label";
import { resolveNewProjectFolder } from "./CreateProjectFolderForm.logic";

/** Names a child folder before using the palette's existing add-project action. */
export function CreateProjectFolderForm(props: {
  readonly parentPath: string;
  readonly entries: ReadonlyArray<FilesystemBrowseEntry>;
  readonly platform: string;
  readonly connected: boolean;
  readonly onCancel: () => void;
  readonly onCreate: (path: string) => Promise<void>;
}) {
  const id = useId();
  const inputRef = useRef<HTMLInputElement>(null);
  const inFlight = useRef(false);
  const [name, setName] = useState("");
  const [pending, setPending] = useState(false);
  const result = resolveNewProjectFolder({ ...props, name });

  useLayoutEffect(() => {
    inputRef.current?.focus();
  }, []);

  return (
    <form
      className="flex flex-col gap-4 p-4"
      aria-label="Create folder"
      onKeyDown={(event) => {
        if (event.key === "Escape" && !pending) {
          event.preventDefault();
          event.stopPropagation();
          props.onCancel();
        }
      }}
      onSubmit={async (event) => {
        event.preventDefault();
        if (!result.path || !props.connected || inFlight.current) return;
        inFlight.current = true;
        setPending(true);
        try {
          await props.onCreate(result.path);
        } finally {
          inFlight.current = false;
          setPending(false);
        }
      }}
    >
      <div>
        <h2 className="font-medium">Create folder</h2>
        <p className="break-all text-sm text-muted-foreground">In {props.parentPath}</p>
      </div>
      <div className="flex flex-col gap-2">
        <Label htmlFor={id}>Folder name</Label>
        <Input
          ref={inputRef}
          id={id}
          value={name}
          placeholder="my-project"
          disabled={pending}
          aria-invalid={name.length > 0 && result.error !== null}
          aria-describedby={`${id}-description`}
          onChange={(event) => setName(event.target.value)}
        />
        <p id={`${id}-description`} className="text-sm text-muted-foreground" aria-live="polite">
          {!props.connected
            ? "Connect to this environment to create a project."
            : name.length > 0 && result.error
              ? result.error
              : "The new folder will be added as a project."}
        </p>
      </div>
      <div className="flex justify-end gap-2">
        <Button type="button" variant="outline" disabled={pending} onClick={props.onCancel}>
          Cancel
        </Button>
        <Button type="submit" disabled={!result.path || !props.connected || pending}>
          {pending ? "Creating…" : "Create & Add project"}
        </Button>
      </div>
    </form>
  );
}
