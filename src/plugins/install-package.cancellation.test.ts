/**
 * Regressions for startup cancellation reaching an archive's inner package install.
 * The archive wrapper builds a fresh options object for the extracted package, so
 * the abort signal has to be forwarded explicitly or SIGTERM during plugin
 * convergence leaves the extracted package install running until it finishes.
 */
import { describe, expect, it, vi } from "vitest";

const withExtractedArchiveRootMock = vi.fn();

vi.mock("./install.runtime.js", async () => {
  const actual =
    await vi.importActual<typeof import("./install.runtime.js")>("./install.runtime.js");
  return {
    ...actual,
    resolveArchiveSourcePath: async () => ({ ok: true, path: "/fake/plugin.tgz" }),
    withExtractedArchiveRoot: (...args: unknown[]) => withExtractedArchiveRootMock(...args),
  };
});

const { installPluginFromArchive } = await import("./install-package.js");

describe("installPluginFromArchive startup cancellation", () => {
  it("forwards the abort signal into the extracted package install", async () => {
    const controller = new AbortController();
    const reason = new Error("Gateway startup interrupted by SIGTERM");
    // Cancellation lands inside the extraction window: the extracted package
    // install starts there and is the layer that observes the abort.
    withExtractedArchiveRootMock.mockImplementationOnce(
      async (params: { onExtracted: (rootDir: string) => Promise<unknown> }) => {
        controller.abort(reason);
        return await params.onExtracted("/fake/extracted");
      },
    );

    const failure = await installPluginFromArchive({
      archivePath: "/fake/plugin.tgz",
      signal: controller.signal,
    }).then(
      () => null,
      (error: unknown) => error,
    );

    // Without the forwarded signal the extracted package install keeps running
    // and the archive flow fails later on the fake source tree instead.
    expect(failure).toBe(reason);
  });
});
