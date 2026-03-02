import Redis from 'ioredis'
import { nanoid } from 'nanoid'

import { RedisStreams } from '../src'

type Subscription = {
  unsubscribe: () => void
}

const redisUrl = process.env.REDIS_URL || 'redis://127.0.0.1:6379/0'

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

async function waitFor(
  condition: () => boolean | Promise<boolean>,
  timeoutMs: number,
  intervalMs = 25,
  label = 'condition'
): Promise<void> {
  const startedAt = Date.now()

  while (!(await condition())) {
    if (Date.now() - startedAt > timeoutMs) {
      throw new Error(`Timed out waiting for ${label}`)
    }

    await sleep(intervalMs)
  }
}

async function waitForPendingCount(
  redis: Redis,
  streamName: string,
  groupName: string,
  expectedCount: number,
  timeoutMs: number
): Promise<void> {
  await waitFor(async () => {
    const summary = (await redis.xpending(streamName, groupName)) as unknown as [number]
    return Number(summary?.[0] || 0) === expectedCount
  }, timeoutMs, 25, `pending count ${expectedCount}`)
}

async function supportsXAutoClaim(redis: Redis): Promise<boolean> {
  try {
    const commandInfo = (await redis.call('COMMAND', 'INFO', 'XAUTOCLAIM')) as
      | unknown[]
      | null

    if (!Array.isArray(commandInfo) || commandInfo.length === 0) {
      return false
    }

    return commandInfo[0] !== null
  } catch (_error) {
    return false
  }
}

async function main(): Promise<void> {
  const runId = nanoid(8)

  const streamMulti = `example:multi:${runId}`
  const groupA = `example:group:a:${runId}`
  const groupB = `example:group:b:${runId}`

  const streamPending = `example:pending:${runId}`
  const pendingGroup = `example:group:pending:${runId}`

  const publisherRedis = new Redis(redisUrl)
  const streams = new RedisStreams(publisherRedis)

  const subscriptions: Subscription[] = []

  try {
    console.log('Running Redis Streams harness...')
    console.log(`Redis URL: ${redisUrl}`)
    console.log(`Run ID: ${runId}`)

    // Scenario 1: multiple consumer instances in one group + independent group fanout
    console.log('\nScenario 1: multi-instance group processing and fanout')

    const groupAMessageIds = new Set<string>()
    const groupBMessageIds = new Set<string>()

    const groupAConsumer1 = streams.subscribe<{ id: string }>(
      streamMulti,
      groupA,
      async ({ message, ack }) => {
        groupAMessageIds.add(message.id)
        await ack()
      },
      {
        subscribeFromStart: true,
        consumerName: `svc-a:instance-1:${runId}`,
        pollInterval: 200,
        onError: (error) => {
          console.error('groupA consumer1 error', error.message)
        },
      }
    )

    const groupAConsumer2 = streams.subscribe<{ id: string }>(
      streamMulti,
      groupA,
      ({ message, ack }) => {
        groupAMessageIds.add(message.id)
        void ack().catch((error) => {
          console.error('groupA consumer2 ack failed', error.message)
        })
      },
      {
        subscribeFromStart: true,
        consumerName: `svc-a:instance-2:${runId}`,
        pollInterval: 200,
        onError: (error) => {
          console.error('groupA consumer2 error', error.message)
        },
      }
    )

    const groupBConsumer1 = streams.subscribe<{ id: string }>(
      streamMulti,
      groupB,
      ({ message, ack }) => {
        groupBMessageIds.add(message.id)
        void ack().catch((error) => {
          console.error('groupB consumer1 ack failed', error.message)
        })
      },
      {
        subscribeFromStart: true,
        consumerName: `svc-b:instance-1:${runId}`,
        pollInterval: 200,
        onError: (error) => {
          console.error('groupB consumer1 error', error.message)
        },
      }
    )

    subscriptions.push(groupAConsumer1, groupAConsumer2, groupBConsumer1)

    await sleep(250)

    const totalMessages = 6
    const publishedMessageIds = Array.from({ length: totalMessages }, (_, index) => {
      return `msg-${index + 1}`
    })

    for (const id of publishedMessageIds) {
      await streams.publish({ id }, streamMulti)
    }

    await waitFor(
      () => groupAMessageIds.size === totalMessages,
      5000,
      25,
      'group A to process all messages'
    )
    await waitFor(
      () => groupBMessageIds.size === totalMessages,
      5000,
      25,
      'group B to process all messages'
    )

    console.log(`- Group A processed ${groupAMessageIds.size}/${totalMessages} messages`)
    console.log(`- Group B processed ${groupBMessageIds.size}/${totalMessages} messages`)

    // Scenario 2: pending message is not recovered by different consumer identity
    console.log('\nScenario 2: pending message is not recovered by a different consumer')

    let firstConsumerReceived = false
    const originalConsumerName = `svc-pending:instance-1:${runId}`

    const firstConsumer = streams.subscribe<{ id: string }>(
      streamPending,
      pendingGroup,
      async ({ message }) => {
        if (message.id === 'pending-1') {
          firstConsumerReceived = true
        }
      },
      {
        subscribeFromStart: true,
        consumerName: originalConsumerName,
        pollInterval: 200,
      }
    )

    subscriptions.push(firstConsumer)

    await sleep(150)

    await streams.publish({ id: 'pending-1' }, streamPending)

    await waitFor(() => firstConsumerReceived, 5000, 25, 'first consumer to receive pending message')

    firstConsumer.unsubscribe()

    await waitForPendingCount(publisherRedis, streamPending, pendingGroup, 1, 3000)

    let differentConsumerReceived = false
    const differentConsumer = streams.subscribe<{ id: string }>(
      streamPending,
      pendingGroup,
      async ({ message, ack }) => {
        differentConsumerReceived = true
        await ack()
        console.log(`- Unexpected pickup by different consumer: ${message.id}`)
      },
      {
        subscribeFromStart: false,
        consumerName: `svc-pending:instance-2:${runId}`,
        pollInterval: 200,
        recoverPending: true,
      }
    )

    subscriptions.push(differentConsumer)

    await sleep(1000)

    if (differentConsumerReceived) {
      throw new Error('Expected different consumer identity not to pick pending message')
    }

    await waitForPendingCount(publisherRedis, streamPending, pendingGroup, 1, 3000)
    console.log('- Different consumer did not pick the pending message (expected)')
    differentConsumer.unsubscribe()

    // Scenario 3: same consumer identity recovers its own pending message
    console.log('\nScenario 3: same consumer identity recovers pending message')

    let recoveredBySameIdentity = false
    const sameConsumerRecovery = streams.subscribe<{ id: string }>(
      streamPending,
      pendingGroup,
      async ({ message, ack }) => {
        if (message.id === 'pending-1') {
          recoveredBySameIdentity = true
        }
        await ack()
      },
      {
        subscribeFromStart: false,
        consumerName: originalConsumerName,
        pollInterval: 200,
        recoverPending: true,
      }
    )

    subscriptions.push(sameConsumerRecovery)

    await waitFor(
      () => recoveredBySameIdentity,
      5000,
      25,
      'same consumer identity to recover pending message'
    )
    await waitForPendingCount(publisherRedis, streamPending, pendingGroup, 0, 3000)

    console.log('- Same consumer identity successfully recovered and acked pending message')
    sameConsumerRecovery.unsubscribe()

    // Scenario 4: different identity can recover pending when reclaim is enabled
    console.log('\nScenario 4: different identity recovers pending via XAUTOCLAIM mode')

    const canUseXAutoClaim = await supportsXAutoClaim(publisherRedis)
    if (!canUseXAutoClaim) {
      console.log('- Skipped: Redis server does not support XAUTOCLAIM')
      console.log('\nHarness completed successfully')
      return
    }

    let seedConsumerReceived = false
    const seedConsumerName = `svc-pending:seed:${runId}`

    const seedConsumer = streams.subscribe<{ id: string }>(
      streamPending,
      pendingGroup,
      async ({ message }) => {
        if (message.id === 'pending-2') {
          seedConsumerReceived = true
        }
      },
      {
        subscribeFromStart: false,
        consumerName: seedConsumerName,
        pollInterval: 200,
      }
    )

    subscriptions.push(seedConsumer)

    await sleep(150)
    await streams.publish({ id: 'pending-2' }, streamPending)
    await waitFor(() => seedConsumerReceived, 5000, 25, 'seed consumer to receive pending-2')

    seedConsumer.unsubscribe()
    await waitForPendingCount(publisherRedis, streamPending, pendingGroup, 1, 3000)

    let reclaimedByDifferentIdentity = false
    const reclaimConsumer = streams.subscribe<{ id: string }>(
      streamPending,
      pendingGroup,
      async ({ message, ack }) => {
        if (message.id === 'pending-2') {
          reclaimedByDifferentIdentity = true
        }
        await ack()
      },
      {
        subscribeFromStart: false,
        consumerName: `svc-pending:reclaimer:${runId}`,
        pollInterval: 200,
        recoverPending: true,
        reclaimPendingFromOtherConsumers: true,
        reclaimMinIdleTimeMs: 50,
        onError: (error) => {
          console.error('reclaim consumer error', error.message)
        },
      }
    )

    subscriptions.push(reclaimConsumer)

    await waitFor(
      () => reclaimedByDifferentIdentity,
      5000,
      25,
      'different identity to reclaim pending-2'
    )
    await waitForPendingCount(publisherRedis, streamPending, pendingGroup, 0, 3000)

    console.log('- Different consumer identity reclaimed pending message when enabled')

    console.log('\nHarness completed successfully')
  } finally {
    for (const subscription of subscriptions) {
      subscription.unsubscribe()
    }

    await publisherRedis.quit()
  }
}

void main().catch((error) => {
  console.error('Harness failed')
  console.error(error)
  process.exitCode = 1
})
