// @vitest-environment jsdom

import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { CreateProjectFolderForm } from "./CreateProjectFolderForm";

let root: Root;
let container: HTMLDivElement;

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
});

afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
});

async function renderForm(onCreate: (path: string) => Promise<void>, connected = true) {
  const onCancel = vi.fn();
  await act(async () => {
    root.render(
      <CreateProjectFolderForm
        parentPath="/remote/projects"
        entries={[]}
        platform="Linux"
        connected={connected}
        onCreate={onCreate}
        onCancel={onCancel}
      />,
    );
  });
  return onCancel;
}

async function typeName(name: string) {
  const input = container.querySelector("input");
  if (!input) throw new Error("Missing folder name input");
  await act(async () => {
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")?.set?.call(input, name);
    input.dispatchEvent(new Event("input", { bubbles: true }));
  });
}

function submit() {
  container
    .querySelector("form")
    ?.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
}

describe("CreateProjectFolderForm interaction", () => {
  it("focuses the name field and prevents blank or traversal submissions", async () => {
    const create = vi.fn(async () => {});
    await renderForm(create);
    expect(document.activeElement).toBe(container.querySelector("input"));
    await act(async () => submit());
    await typeName("../escape");
    await act(async () => submit());
    expect(create).not.toHaveBeenCalled();
    expect(container.textContent).toContain("Enter a folder name, not a path.");
  });

  it("allows only one creation while the request is pending, then allows retry", async () => {
    let finishRequest = () => {};
    const request = new Promise<void>((resolve) => {
      finishRequest = resolve;
    });
    const create = vi.fn(() => request);
    await renderForm(create);
    await typeName("my project");
    await act(async () => {
      submit();
      submit();
    });
    expect(create).toHaveBeenCalledExactlyOnceWith("/remote/projects/my project");
    expect(container.querySelector("input")?.disabled).toBe(true);
    await act(async () => finishRequest());
    expect(container.querySelector("input")?.disabled).toBe(false);
    await act(async () => submit());
    expect(create).toHaveBeenCalledTimes(2);
  });

  it("blocks submission after disconnect and allows it after reconnect", async () => {
    const create = vi.fn(async () => {});
    await renderForm(create);
    await typeName("new-project");
    await renderForm(create, false);
    await act(async () => submit());
    expect(create).not.toHaveBeenCalled();
    expect(container.textContent).toContain("Connect to this environment");
    await renderForm(create);
    await act(async () => submit());
    expect(create).toHaveBeenCalledExactlyOnceWith("/remote/projects/new-project");
  });

  it("cancels with Escape without submitting or bubbling to the palette dialog", async () => {
    const create = vi.fn(async () => {});
    const cancel = await renderForm(create);
    await typeName("discard-me");
    const outerKeyDown = vi.fn();
    document.addEventListener("keydown", outerKeyDown);
    await act(async () => {
      container
        .querySelector("input")
        ?.dispatchEvent(
          new KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true }),
        );
    });
    expect(cancel).toHaveBeenCalledOnce();
    expect(create).not.toHaveBeenCalled();
    expect(outerKeyDown).not.toHaveBeenCalled();
    document.removeEventListener("keydown", outerKeyDown);
  });
});
