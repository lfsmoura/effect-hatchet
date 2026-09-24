/**
 * Vitest global setup for the e2e suite: resolves the Hatchet engine to test
 * against and hands its connection details to the tests via `inject("hatchet")`.
 *
 * - With HATCHET_CLIENT_TOKEN (or a .hatchet-token file from
 *   scripts/hatchet-token.sh), tests run against that engine, e.g. the
 *   docker-compose hatchet-lite.
 * - Otherwise an embedded Hatchet engine with a bundled Postgres is started
 *   (the sidecar binary is downloaded and cached under ~/.hatchet/embedded on
 *   first use), so no Docker or token is needed.
 */
import { startEmbeddedSidecar } from "@hatchet-dev/typescript-sdk/v1/embedded.js"
import { existsSync, readFileSync } from "node:fs"
import type { TestProject } from "vitest/node"

export interface HatchetConnection {
  readonly token: string
  readonly hostPort?: string
}

declare module "vitest" {
  export interface ProvidedContext {
    hatchet: HatchetConnection
  }
}

const tokenFile = new URL("../.hatchet-token", import.meta.url).pathname
const externalToken = process.env.HATCHET_CLIENT_TOKEN ??
  (existsSync(tokenFile) ? readFileSync(tokenFile, "utf8").trim() : undefined)

// The sidecar's stop promise can stay pending after the engine has exited;
// the sidecar also shuts down with this process, so teardown never blocks on it.
const stopTimeoutMs = 15_000

export default async function setup(project: TestProject) {
  if (externalToken !== undefined) {
    project.provide("hatchet", {
      token: externalToken,
      ...(process.env.HATCHET_CLIENT_HOST_PORT !== undefined
        ? { hostPort: process.env.HATCHET_CLIENT_HOST_PORT }
        : {})
    })
    return
  }

  const sidecar = await startEmbeddedSidecar({ logLevel: "warn" })
  project.provide("hatchet", { token: sidecar.token, hostPort: sidecar.grpcAddress })

  return async () => {
    let timer: NodeJS.Timeout | undefined
    await Promise.race([
      sidecar.stop(),
      new Promise<void>((resolve) => {
        timer = setTimeout(resolve, stopTimeoutMs)
      })
    ])
    clearTimeout(timer)
  }
}
