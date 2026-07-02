import type { TestProject } from 'vitest/node'

declare module 'vitest' {
  export interface ProvidedContext {
    redisUrl: string
  }
}

export default async function setup(project: TestProject) {
  // Allow pointing tests at an existing Redis (e.g. local dev) instead of Docker.
  if (process.env.REDIS_URL) {
    project.provide('redisUrl', process.env.REDIS_URL)
    return
  }

  const { RedisContainer } = await import('@testcontainers/redis')
  const container = await new RedisContainer('redis:7-alpine').start()
  project.provide('redisUrl', container.getConnectionUrl())

  return async () => {
    await container.stop()
  }
}
