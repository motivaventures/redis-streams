import Redis from 'ioredis'
import { nanoid } from 'nanoid'
import { describe, expect, it } from 'vitest'
import { RedisStreams } from '.'

const describeWithRedis =
  process.env.REDIS_INTEGRATION === '1' ? describe : describe.skip

describeWithRedis('Redis Streams', () => {
  it('should work', async () => {
    const redis = new Redis()
    const streams = new RedisStreams(redis)

    const streamName = `STREAM_${nanoid()}`
    const group1 = `GROUP_${nanoid()}`
    const group2 = `GROUP_${nanoid()}`

    await streams.publish(
      {
        message: 'hi',
      },
      streamName
    )
    await streams.publish(
      {
        message: 'hi2',
      },
      streamName
    )

    let count = 0
    const sub1 = streams.subscribe(
      streamName,
      group1,
      async ({ ack }) => {
        await ack()
        count++
      },
      {
        subscribeFromStart: true,
      }
    )

    const sub2 = streams.subscribe(
      streamName,
      group2,
      async ({ ack }) => {
        await ack()
        count++
      },
      {
        subscribeFromStart: true,
      }
    )

    try {
      await new Promise<void>((resolve, reject) => {
        const checkDone = () => {
          if (count === 4) {
            resolve()
            return
          }

          setTimeout(checkDone, 10)
        }

        setTimeout(checkDone, 10)
        setTimeout(
          () => reject(new Error('Did not process all messages in time')),
          9000
        )
      })

      expect(count).toEqual(4)
    } finally {
      sub1.unsubscribe()
      sub2.unsubscribe()
      await redis.quit()
    }
  }, 10000)
})
