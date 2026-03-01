import { afterEach, describe, expect, it, vi } from 'vitest'

import { RedisStreams } from '.'

type MockRedis = {
  xgroup: ReturnType<typeof vi.fn>
  xreadgroup: ReturnType<typeof vi.fn>
  xack: ReturnType<typeof vi.fn>
}

const makeMessage = (streamName: string, id: string, payload: unknown) => [
  [streamName, [[id, ['json', JSON.stringify(payload)]]]],
]

const createRedisMock = (): MockRedis => ({
  xgroup: vi.fn().mockResolvedValue('OK'),
  xreadgroup: vi.fn().mockResolvedValue(null),
  xack: vi.fn().mockResolvedValue(1),
})

describe('RedisStreams defect coverage', () => {
  const originalHostname = process.env.HOSTNAME

  afterEach(() => {
    process.env.HOSTNAME = originalHostname
    vi.useRealTimers()
    vi.clearAllTimers()
    vi.restoreAllMocks()
  })

  it('should default consumer identity to stable hostname when unset', async () => {
    vi.useFakeTimers()

    process.env.HOSTNAME = 'worker-0'

    const redis = createRedisMock()
    const streams = new RedisStreams(redis as never)

    streams.subscribe('STREAM1', 'GROUP1', () => {}, {
      subscribeFromStart: true,
    })

    await vi.advanceTimersByTimeAsync(0)

    expect(redis.xreadgroup).toHaveBeenCalledTimes(1)
    expect(redis.xreadgroup.mock.calls[0][2]).toBe('worker-0')
  })

  it('should return a subscription handle with unsubscribe', () => {
    const redis = createRedisMock()
    const streams = new RedisStreams(redis as never)

    const subscription = streams.subscribe('STREAM1', 'GROUP1', () => {}, {
      subscribeFromStart: true,
    })

    expect(subscription).toBeDefined()
    expect(typeof (subscription as { unsubscribe?: unknown }).unsubscribe).toBe(
      'function'
    )
  })

  it('should not poll next batch until async handler settles', async () => {
    vi.useFakeTimers()

    const redis = createRedisMock()
    redis.xreadgroup
      .mockResolvedValueOnce(makeMessage('STREAM1', '1-0', { message: 'first' }))
      .mockResolvedValue(null)

    const streams = new RedisStreams(redis as never)

    let releaseFirst: (() => void) | null = null

    streams.subscribe(
      'STREAM1',
      'GROUP1',
      async () => {
        await new Promise<void>((resolve) => {
          releaseFirst = resolve
        })
      },
      { subscribeFromStart: true }
    )

    await vi.advanceTimersByTimeAsync(0)
    await vi.advanceTimersByTimeAsync(0)
    await vi.advanceTimersByTimeAsync(0)

    expect(redis.xreadgroup).toHaveBeenCalledTimes(1)

    releaseFirst?.()
  })

  it('should stop polling after unsubscribe is called', async () => {
    vi.useFakeTimers()

    const redis = createRedisMock()
    redis.xreadgroup.mockResolvedValue(null)

    const streams = new RedisStreams(redis as never)
    const subscription = streams.subscribe('STREAM1', 'GROUP1', () => {}, {
      subscribeFromStart: true,
      pollInterval: 1,
    }) as { unsubscribe: () => void }

    await vi.advanceTimersByTimeAsync(0)
    const callsBeforeUnsubscribe = redis.xreadgroup.mock.calls.length

    subscription.unsubscribe()

    await vi.advanceTimersByTimeAsync(10)
    expect(redis.xreadgroup.mock.calls.length).toBe(callsBeforeUnsubscribe)
  })

  it('should keep polling after transient read errors', async () => {
    vi.useFakeTimers()

    const redis = createRedisMock()
    redis.xreadgroup
      .mockRejectedValueOnce(new Error('temporary redis failure'))
      .mockResolvedValueOnce(null)

    const onError = vi.fn()

    const streams = new RedisStreams(redis as never)
    streams.subscribe('STREAM1', 'GROUP1', () => {}, {
      subscribeFromStart: true,
      retryDelayMs: 5,
      onError,
    })

    await vi.advanceTimersByTimeAsync(0)
    expect(onError).toHaveBeenCalledTimes(1)
    expect(redis.xreadgroup).toHaveBeenCalledTimes(1)

    await vi.advanceTimersByTimeAsync(5)
    expect(redis.xreadgroup).toHaveBeenCalledTimes(2)
  })

  it('should recover pending messages before reading new ones', async () => {
    vi.useFakeTimers()

    const redis = createRedisMock()
    redis.xreadgroup.mockResolvedValue(null)

    const streams = new RedisStreams(redis as never)
    streams.subscribe('STREAM1', 'GROUP1', () => {}, {
      subscribeFromStart: true,
    })

    await vi.advanceTimersByTimeAsync(0)

    expect(redis.xreadgroup).toHaveBeenCalledTimes(1)
    expect(redis.xreadgroup.mock.calls[0][9]).toBe('0-0')
  })
})
