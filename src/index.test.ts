import Redis from 'ioredis'
import { nanoid } from 'nanoid'
import { afterEach, beforeEach, describe, expect, inject, it } from 'vitest'
import { RedisStreams, SubscribeErrorContext, Subscription } from '.'

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

async function waitFor(
  condition: () => boolean | Promise<boolean>,
  label: string,
  timeoutMs = 10000
) {
  const start = Date.now()
  while (!(await condition())) {
    if (Date.now() - start > timeoutMs) {
      throw new Error(`Timed out waiting for ${label}`)
    }
    await delay(25)
  }
}

describe('RedisStreams', () => {
  let redis: Redis
  let streams: RedisStreams
  let subscriptions: Subscription[]
  let streamName: string
  let groupName: string

  const track = (sub: Subscription) => {
    subscriptions.push(sub)
    return sub
  }

  const waitForGroup = (stream: string, group: string) =>
    waitFor(async () => {
      try {
        const groups = (await redis.xinfo('GROUPS', stream)) as unknown[][]
        return groups.some((g) => g.includes(group))
      } catch {
        return false
      }
    }, `group ${group} on ${stream}`)

  const pendingCount = async (stream: string, group: string) => {
    const summary = (await redis.xpending(stream, group)) as unknown[]
    return Number(summary[0])
  }

  beforeEach(() => {
    redis = new Redis(inject('redisUrl'))
    streams = new RedisStreams(redis)
    subscriptions = []
    streamName = `stream-${nanoid()}`
    groupName = `group-${nanoid()}`
  })

  afterEach(async () => {
    await Promise.all(subscriptions.map((sub) => sub.stop()))
    await redis.quit().catch(() => {})
  })

  it('delivers the exact published payload', async () => {
    const payload = { message: 'hi', nested: { n: 1, list: ['a', 'b'] } }
    await streams.publish(payload, streamName)

    const received: unknown[] = []
    track(
      streams.subscribe<typeof payload>(
        streamName,
        groupName,
        async ({ message, ack }) => {
          await ack()
          received.push(message)
        },
        { subscribeFromStart: true, pollInterval: 200 }
      )
    )

    await waitFor(() => received.length === 1, 'message delivery')
    expect(received[0]).toEqual(payload)
    await waitFor(
      async () => (await pendingCount(streamName, groupName)) === 0,
      'ack to clear pending'
    )
  })

  it('fans out every message to each group', async () => {
    await streams.publish({ message: 'hi' }, streamName)
    await streams.publish({ message: 'hi2' }, streamName)

    const byGroup: Record<string, string[]> = { GROUP1: [], GROUP2: [] }
    for (const group of ['GROUP1', 'GROUP2']) {
      track(
        streams.subscribe<{ message: string }>(
          streamName,
          `${groupName}-${group}`,
          async ({ message, ack }) => {
            await ack()
            byGroup[group].push(message.message)
          },
          { subscribeFromStart: true, pollInterval: 200 }
        )
      )
    }

    await waitFor(
      () => byGroup.GROUP1.length === 2 && byGroup.GROUP2.length === 2,
      'both groups to receive both messages'
    )
    expect(byGroup.GROUP1.sort()).toEqual(['hi', 'hi2'])
    expect(byGroup.GROUP2.sort()).toEqual(['hi', 'hi2'])
  })

  it('shares work across consumers in the same group without duplicates', async () => {
    const values = [1, 2, 3, 4, 5, 6]
    for (const n of values) {
      await streams.publish({ n }, streamName)
    }

    const received: number[] = []
    for (const consumer of ['a', 'b']) {
      track(
        streams.subscribe<{ n: number }>(
          streamName,
          groupName,
          async ({ message, ack }) => {
            await ack()
            received.push(message.n)
          },
          {
            subscribeFromStart: true,
            pollInterval: 100,
            consumerName: consumer,
          }
        )
      )
    }

    await waitFor(() => received.length >= values.length, 'all messages')
    await delay(300)
    expect(received.sort()).toEqual(values)
  })

  it('only receives messages published after subscribing by default', async () => {
    await streams.publish({ phase: 'before' }, streamName)

    const received: Array<{ phase: string }> = []
    track(
      streams.subscribe<{ phase: string }>(
        streamName,
        groupName,
        async ({ message, ack }) => {
          await ack()
          received.push(message)
        },
        { subscribeFromStart: false, pollInterval: 100 }
      )
    )

    await waitForGroup(streamName, groupName)
    await streams.publish({ phase: 'after' }, streamName)

    await waitFor(() => received.length === 1, 'post-subscribe message')
    await delay(300)
    expect(received).toEqual([{ phase: 'after' }])
  })

  it('reads history when subscribeFromStart is true', async () => {
    await streams.publish({ n: 1 }, streamName)
    await streams.publish({ n: 2 }, streamName)

    const received: number[] = []
    track(
      streams.subscribe<{ n: number }>(
        streamName,
        groupName,
        async ({ message, ack }) => {
          await ack()
          received.push(message.n)
        },
        { subscribeFromStart: true, pollInterval: 100 }
      )
    )

    await waitFor(() => received.length === 2, 'historical messages')
    expect(received.sort()).toEqual([1, 2])
  })

  it('stop() resolves promptly mid-block and halts delivery', async () => {
    const received: unknown[] = []
    const sub = track(
      streams.subscribe(
        streamName,
        groupName,
        ({ message }) => {
          received.push(message)
        },
        { subscribeFromStart: true, pollInterval: 5000 }
      )
    )
    await waitForGroup(streamName, groupName)

    const start = Date.now()
    await sub.stop()
    expect(Date.now() - start).toBeLessThan(1000)

    await streams.publish({ late: true }, streamName)
    await delay(300)
    expect(received).toEqual([])

    // stop() is idempotent
    await sub.stop()
  })

  it('reports handler errors, keeps consuming, and leaves the message pending', async () => {
    await streams.publish({ n: 1 }, streamName)
    await streams.publish({ n: 2 }, streamName)

    const received: number[] = []
    const errors: Array<{ err: unknown; context: SubscribeErrorContext }> = []
    track(
      streams.subscribe<{ n: number }>(
        streamName,
        groupName,
        async ({ message, ack }) => {
          if (message.n === 1) {
            throw new Error('handler boom')
          }
          await ack()
          received.push(message.n)
        },
        {
          subscribeFromStart: true,
          pollInterval: 100,
          onError: (err, context) => errors.push({ err, context }),
        }
      )
    )

    await waitFor(() => received.length === 1, 'second message despite failure')
    expect(received).toEqual([2])
    expect(errors).toHaveLength(1)
    expect((errors[0].err as Error).message).toBe('handler boom')
    expect(errors[0].context.messageId).toBeDefined()
    expect(await pendingCount(streamName, groupName)).toBe(1)
  })

  it('reclaims stale pending messages when claimIdleMs is set', async () => {
    await streams.publish({ job: 'stuck' }, streamName)

    const aReceived: unknown[] = []
    const subA = track(
      streams.subscribe(
        streamName,
        groupName,
        ({ message }) => {
          aReceived.push(message) // never acks
        },
        {
          subscribeFromStart: true,
          pollInterval: 100,
          consumerName: 'consumer-a',
        }
      )
    )
    await waitFor(() => aReceived.length === 1, 'consumer A to read')
    await subA.stop()

    const bReceived: unknown[] = []
    track(
      streams.subscribe(
        streamName,
        groupName,
        async ({ message, ack }) => {
          await ack()
          bReceived.push(message)
        },
        {
          subscribeFromStart: true,
          pollInterval: 100,
          consumerName: 'consumer-b',
          claimIdleMs: 50,
        }
      )
    )

    await waitFor(() => bReceived.length === 1, 'consumer B to reclaim')
    expect(bReceived).toEqual([{ job: 'stuck' }])
    await waitFor(
      async () => (await pendingCount(streamName, groupName)) === 0,
      'reclaimed message to be acked'
    )
  })

  it('survives malformed messages and reports them via onError', async () => {
    await redis.xadd(streamName, '*', 'json', '{not valid json')
    await redis.xadd(streamName, '*', 'other-field', 'x')
    await streams.publish({ ok: true }, streamName)

    const received: unknown[] = []
    const errors: unknown[] = []
    track(
      streams.subscribe(
        streamName,
        groupName,
        async ({ message, ack }) => {
          await ack()
          received.push(message)
        },
        {
          subscribeFromStart: true,
          pollInterval: 100,
          onError: (err) => errors.push(err),
        }
      )
    )

    await waitFor(() => received.length === 1, 'valid message after bad ones')
    expect(received).toEqual([{ ok: true }])
    expect(errors).toHaveLength(2)
  })

  it('recovers with backoff after transient read failures', async () => {
    await redis.xgroup('CREATE', streamName, groupName, '$', 'MKSTREAM')

    const received: unknown[] = []
    const errors: unknown[] = []
    track(
      streams.subscribe(
        streamName,
        groupName,
        async ({ message, ack }) => {
          await ack()
          received.push(message)
        },
        {
          subscribeFromStart: false,
          pollInterval: 100,
          disableCreateGroup: true,
          onError: (err) => errors.push(err),
        }
      )
    )
    await waitForGroup(streamName, groupName)

    await redis.xgroup('DESTROY', streamName, groupName)
    await waitFor(() => errors.length >= 1, 'NOGROUP error to surface')

    await redis.xgroup('CREATE', streamName, groupName, '$')
    await streams.publish({ back: true }, streamName)

    await waitFor(() => received.length === 1, 'delivery after recovery')
    expect(received).toEqual([{ back: true }])
  })

  it('validates inputs synchronously', async () => {
    const handler = () => {}
    expect(() => streams.subscribe('', groupName, handler)).toThrow(TypeError)
    expect(() => streams.subscribe(streamName, '', handler)).toThrow(TypeError)
    expect(() =>
      streams.subscribe(streamName, groupName, handler, {
        subscribeFromStart: true,
        pollInterval: 0,
      })
    ).toThrow(TypeError)
    expect(() =>
      streams.subscribe(streamName, groupName, handler, {
        subscribeFromStart: true,
        count: 0,
      })
    ).toThrow(TypeError)
    expect(() =>
      streams.subscribe(streamName, groupName, handler, {
        subscribeFromStart: true,
        claimIdleMs: -1,
      })
    ).toThrow(TypeError)

    await expect(streams.publish({ a: 1 }, '')).rejects.toThrow(TypeError)
    await expect(
      streams.publish({ a: 1 }, streamName, { maxLength: 0 })
    ).rejects.toThrow(TypeError)
  })
})
