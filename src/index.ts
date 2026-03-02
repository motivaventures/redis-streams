import { Redis } from 'ioredis'
import { nanoid } from 'nanoid'

interface SubscribeOpts {
  pollInterval?: number
  subscribeFromStart: boolean
  consumerName?: string
  recoverPending?: boolean
  reclaimPendingFromOtherConsumers?: boolean
  reclaimMinIdleTimeMs?: number
  reclaimBatchSize?: number
  retryDelayMs?: number
  maxRetryDelayMs?: number
  onError?: (error: Error) => void
}

interface Subscription {
  unsubscribe: () => void
}

type SubscriberMode = 'pending' | 'reclaim' | 'tail'

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
    groupName: string,
    messageId: string
  ) {
    await this.redis.xack(streamName, groupName, messageId)
  }

  private parseMessagePayload<T>(rawFields: Array<string | Buffer>): T {
    const asString = (value: string | Buffer): string => {
      return typeof value === 'string' ? value : value.toString('utf8')
    }

    let jsonValue: string | null = null
    for (let i = 0; i < rawFields.length; i += 2) {
      const key = rawFields[i]
      const value = rawFields[i + 1]

      if (key === undefined || value === undefined) {
        continue
      }

      if (asString(key) === 'json') {
        jsonValue = asString(value)
        break
      }
    }

    if (jsonValue === null) {
      throw new Error('Missing json field in stream message')
    }

    return JSON.parse(jsonValue) as T
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
    const reclaimMinIdleTimeMs = opts?.reclaimMinIdleTimeMs ?? 1000
    const reclaimBatchSize = opts?.reclaimBatchSize ?? 20
    const consumerName = opts?.consumerName || process.env.HOSTNAME || nanoid()

    const state = {
      cancelled: false,
      retryDelayMs: initialRetryDelay,
      mode: ((opts?.recoverPending ?? false) ? 'pending' : 'tail') as SubscriberMode,
      groupReady: false,
      reclaimCursor: '0-0',
    }

    const safeOnError = (error: Error) => {
      try {
        opts?.onError?.(error)
      } catch (_ignored) {
        // Intentionally ignore observer callback failures.
      }
    }

    const isGroupAlreadyExistsError = (error: Error): boolean => {
      const message = error.message.toLowerCase()
      return message.includes('busygroup') || message.includes('already exists')
    }

    const poll = (delayMs = 0) => {
      setTimeout(async () => {
        if (state.cancelled) {
          return
        }

        let nextDelayMs = 0

        try {
          if (!state.groupReady) {
            try {
              await this.redis.xgroup(
                'CREATE',
                streamName,
                groupName,
                opts?.subscribeFromStart ? 0 : '$',
                'MKSTREAM'
              )
              state.groupReady = true
            } catch (err) {
              if (err instanceof Error && !isGroupAlreadyExistsError(err)) {
                throw err
              }

              state.groupReady = true
            }
          }

          let data: any[] | null = null // TODO: Better typing
          const hasMessages = (rows: any[] | null): boolean => {
            if (!Array.isArray(rows) || rows.length === 0) {
              return false
            }

            return rows.some((row) => Array.isArray(row?.[1]) && row[1].length > 0)
          }

          if (state.mode === 'reclaim') {
            const reclaimResult = (await (this.redis as any).xautoclaim(
              streamName,
              groupName,
              consumerName,
              reclaimMinIdleTimeMs,
              state.reclaimCursor,
              'COUNT',
              reclaimBatchSize
            )) as [string, Array<[string, Array<string | Buffer>]>]

            const nextCursor = reclaimResult?.[0] || '0-0'
            const reclaimedMessages = reclaimResult?.[1] || []

            state.reclaimCursor = nextCursor

            if (reclaimedMessages.length > 0) {
              data = [[streamName, reclaimedMessages]]
            }

            if (nextCursor === '0-0' && reclaimedMessages.length === 0) {
              state.mode = 'tail'
            }
          } else {
            data = (await this.redis.xreadgroup(
              'GROUP',
              groupName,
              consumerName, // @ts-ignore - appears to be error in types, will have to investigate
              'BLOCK',
              state.mode === 'pending' ? 1 : opts?.pollInterval || 60000,
              'COUNT',
              1,
              'STREAMS',
              streamName,
              state.mode === 'pending' ? '0-0' : '>'
            )) as any[] // TODO: Better typing

            if (state.mode === 'pending' && !hasMessages(data)) {
              state.mode = opts?.reclaimPendingFromOtherConsumers
                ? 'reclaim'
                : 'tail'
            }
          }

          if (state.cancelled) {
            return
          }

          if (hasMessages(data)) {
            const messageRows = data || []
            for (const streams of messageRows) {
              for (const inner of streams[1]) {
                await handler({
                  message: this.parseMessagePayload<T>(inner[1]),
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
          poll(nextDelayMs)
          return
        } catch (err) {
          const error = err instanceof Error ? err : new Error('Unknown error')
          safeOnError(error)
          nextDelayMs = state.retryDelayMs
          state.retryDelayMs = Math.min(state.retryDelayMs * 2, maxRetryDelay)
        }

        poll(nextDelayMs)
      }, delayMs)
    }

    poll()

    return {
      unsubscribe: () => {
        state.cancelled = true
      },
    }
  }
}
