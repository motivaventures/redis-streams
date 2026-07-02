import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['src/**/*.test.ts'],
    globalSetup: ['./vitest.setup.ts'],
    testTimeout: 15000,
    hookTimeout: 120000,
  },
})
