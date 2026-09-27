import { defineConfig } from "vitest/config"

export default defineConfig({
  test: {
    include: ["test/e2e*.test.ts"],
    globalSetup: ["test/e2e.setup.ts"],
    testTimeout: 120_000
  }
})
