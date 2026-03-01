import { Redis } from 'ioredis'
import { nanoid } from 'nanoid'

interface SubscribeOpts {
  pollInterval?: number
  subscribeFromStart: boolean
  consumerName?: string
  disableCreateGroup?: boolean
  retryDelayMs?: number
  maxRetryDelayMs?: number
  onError?: (error: Error) => void
}

interface Subscription {
  unsubscribe: () => void
}

export class RedisStreams {
  constructor(private redis: Redis) {}

  async publish<T>(
    message: T,
    streamName: string,
    opts: {
      maxLength?: number
    } = {}
  ): Promise<void> {
    await this.redis.xadd(
      streamName,
      'MAXLEN',
      '~',
      opts.maxLength || 1000000,
      '*',
      'json',
      JSON.stringify(message)
    )
  }

  private async ackMessage(
    streamName: string,
    consumerName: string,
    messageId: string
  ) {
    await this.redis.xack(streamName, consumerName, messageId)
  }

  subscribe<T>(
    streamName: string,
    groupName: string,
    handler: ({
      message,
      ack,
    }: {
      message: T
      ack: () => Promise<void>
    }) => void | Promise<void>,
    opts?: SubscribeOpts
  ): Subscription {
    const initialRetryDelay = opts?.retryDelayMs ?? 1000
    const maxRetryDelay = opts?.maxRetryDelayMs ?? 30000

    const state = {
      cancelled: false,
      retryDelayMs: initialRetryDelay,
    }

    const poll = (pollOpts?: SubscribeOpts, delayMs = 0) => {
      setTimeout(async () => {
        if (state.cancelled) {
          return
        }

        let nextDelayMs = 0

        try {
          if (!pollOpts?.disableCreateGroup) {
            try {
              await this.redis.xgroup(
                'CREATE',
                streamName,
                groupName,
                pollOpts?.subscribeFromStart ? 0 : '$',
                'MKSTREAM'
              )
            } catch (err) {
              if (
                err instanceof Error &&
                !err.message.toLowerCase().includes('already exists')
              ) {
                throw err
              }
            }
          }

          const consumerName =
            pollOpts?.consumerName || process.env.HOSTNAME || nanoid()
          const data = (await this.redis.xreadgroup(
            'GROUP',
            groupName,
            consumerName, // @ts-ignore - appears to be error in types, will have to investigate
            'BLOCK',
            pollOpts?.pollInterval || 60000,
            'COUNT',
            1,
            'STREAMS',
            streamName,
            '>'
          )) as any[] // TODO: Better typing

          if (state.cancelled) {
            return
          }

          if (data) {
            for (const streams of data) {
              for (const inner of streams[1]) {
                await handler({
                  message: JSON.parse(inner[1][1]),
                  ack: () => {
                    return this.ackMessage(streamName, groupName, inner[0])
                  },
                })
              }
            }
          }

          if (state.cancelled) {
            return
          }

          state.retryDelayMs = initialRetryDelay

          poll(
            {
              subscribeFromStart: false,
              ...pollOpts,
              consumerName,
              disableCreateGroup: true,
            },
            nextDelayMs
          )
          return
        } catch (err) {
          const error = err instanceof Error ? err : new Error('Unknown error')
          pollOpts?.onError?.(error)
          nextDelayMs = state.retryDelayMs
          state.retryDelayMs = Math.min(state.retryDelayMs * 2, maxRetryDelay)
        }

        poll({
          ...pollOpts,
          disableCreateGroup: true,
        }, nextDelayMs)
      }, delayMs)
    }

    poll(opts)

    return {
      unsubscribe: () => {
        state.cancelled = true
      },
    }
  }
}
