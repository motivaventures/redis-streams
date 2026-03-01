import Redis from 'ioredis'
import { describe, expect, it } from 'vitest'
import { RedisStreams } from '.'

describe('Redis Streams', () => {
  it('should work', async () => {
    const redis = new Redis()
    const streams = new RedisStreams(redis)
    await streams.publish(
      {
        message: 'hi',
      },
      'STREAM1'
    )
    await streams.publish(
      {
        message: 'hi2',
      },
      'STREAM1'
    )

    let count = 0
    await new Promise<void>((resolve) => {
      const resolveIfDone = () => {
        if (count === 4) {
          resolve()
        }
      }
      streams.subscribe(
        'STREAM1',
        'GROUP1',
        ({ ack }) => {
          ack()
          count++
          resolveIfDone()
        },
        {
          subscribeFromStart: true,
        }
      )
      streams.subscribe('STREAM1', 'GROUP2', ({ ack }) => {
        ack()
        count++
        resolveIfDone()
      })
    })
    expect(count).toEqual(4)
  }, 5000)
})
