import { Redis } from 'ioredis'
import { nanoid } from 'nanoid'

export interface SubscribeErrorContext {
  streamName: string
  groupName: string
  consumerName: string
  /** Present when the error relates to a specific message (parse or handler failure). */
  messageId?: string
}

export interface SubscribeOpts {
  /** How long each blocking read waits for new messages, in milliseconds. Default 60000. */
  pollInterval?: number
  subscribeFromStart: boolean
  consumerName?: string
  disableCreateGroup?: boolean
  /** Max messages fetched per read. Default 1. */
  count?: number
  /**
   * When set, messages left pending (delivered but not acked) by any consumer
   * of the group for longer than this many milliseconds are claimed by this
   * consumer and redelivered to the handler. Off by default.
   */
  claimIdleMs?: number
  /**
   * Called whenever the subscription hits an error (read failure, unparseable
   * message, throwing handler). The subscription keeps running; read failures
   * are retried with exponential backoff.
   */
  onError?: (err: unknown, context: SubscribeErrorContext) => void
}

export interface Subscription {
  /** Stops the subscription and closes its connection. Safe to call more than once. */
  stop: () => Promise<void>
}

export type MessageHandler<T> = (args: {
  message: T
  ack: () => Promise<void>
}) => void | Promise<void>

/** [id, flat field-value list]; fields are null for entries deleted from the stream. */
type StreamEntry = [id: string, fields: string[] | null]

const INITIAL_BACKOFF_MS = 100
const MAX_BACKOFF_MS = 5000

function assertNonEmptyString(value: string, name: string) {
  if (typeof value !== 'string' || value.length === 0) {
    throw new TypeError(`${name} must be a non-empty string`)
  }
}

function assertPositiveInteger(value: number, name: string) {
  if (!Number.isInteger(value) || value <= 0) {
    throw new TypeError(`${name} must be a positive integer, got ${value}`)
  }
}

function isBusyGroupError(err: unknown): boolean {
  return err instanceof Error && err.message.startsWith('BUSYGROUP')
}

function getField(fields: string[], name: string): string | undefined {
  for (let i = 0; i + 1 < fields.length; i += 2) {
    if (fields[i] === name) {
      return fields[i + 1]
    }
  }
  return undefined
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
    assertNonEmptyString(streamName, 'streamName')
    const maxLength = opts.maxLength ?? 1000000
    assertPositiveInteger(maxLength, 'maxLength')
    await this.redis.xadd(
      streamName,
      'MAXLEN',
      '~',
      maxLength,
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

  subscribe<T>(
    streamName: string,
    groupName: string,
    handler: MessageHandler<T>,
    opts?: SubscribeOpts
  ): Subscription {
    assertNonEmptyString(streamName, 'streamName')
    assertNonEmptyString(groupName, 'groupName')
    const blockMs = opts?.pollInterval ?? 60000
    const count = opts?.count ?? 1
    assertPositiveInteger(blockMs, 'pollInterval')
    assertPositiveInteger(count, 'count')
    const claimIdleMs = opts?.claimIdleMs
    if (claimIdleMs !== undefined) {
      assertPositiveInteger(claimIdleMs, 'claimIdleMs')
    }
    const consumerName = opts?.consumerName ?? nanoid()

    // Dedicated connection: blocking reads must not stall other commands
    // (publish/ack) issued on the instance the caller handed us.
    const conn = this.redis.duplicate()
    let stopped = false
    let wake: (() => void) | undefined

    const reportError = (err: unknown, messageId?: string) => {
      opts?.onError?.(err, { streamName, groupName, consumerName, messageId })
    }

    const sleep = (ms: number) =>
      new Promise<void>((resolve) => {
        const timer = setTimeout(() => {
          wake = undefined
          resolve()
        }, ms)
        wake = () => {
          clearTimeout(timer)
          wake = undefined
          resolve()
        }
      })

    const dispatchEntries = async (entries: StreamEntry[]) => {
      for (const [id, fields] of entries) {
        if (stopped) return
        if (!fields) continue
        const raw = getField(fields, 'json')
        if (raw === undefined) {
          reportError(new Error(`Message ${id} has no "json" field`), id)
          continue
        }
        let message: T
        try {
          message = JSON.parse(raw)
        } catch (err) {
          reportError(err, id)
          continue
        }
        try {
          await handler({
            message,
            ack: () => {
              return this.ackMessage(streamName, groupName, id)
            },
          })
        } catch (err) {
          reportError(err, id)
        }
      }
    }

    let groupPending = !opts?.disableCreateGroup
    const loop = (async () => {
      let backoffMs = INITIAL_BACKOFF_MS
      while (!stopped) {
        try {
          if (groupPending) {
            try {
              await conn.xgroup(
                'CREATE',
                streamName,
                groupName,
                opts?.subscribeFromStart ? 0 : '$',
                'MKSTREAM'
              )
            } catch (err) {
              if (!isBusyGroupError(err)) {
                throw err
              }
            }
            groupPending = false
          }

          if (claimIdleMs !== undefined) {
            const claimReply = (await conn.xautoclaim(
              streamName,
              groupName,
              consumerName,
              claimIdleMs,
              '0',
              'COUNT',
              count
            )) as [cursor: string, entries: StreamEntry[]]
            await dispatchEntries(claimReply[1] ?? [])
          }

          const readReply = (await conn.xreadgroup(
            'GROUP',
            groupName,
            consumerName,
            'COUNT',
            count,
            'BLOCK',
            blockMs,
            'STREAMS',
            streamName,
            '>'
          )) as [stream: string, entries: StreamEntry[]][] | null

          if (readReply) {
            for (const [, entries] of readReply) {
              await dispatchEntries(entries)
            }
          }
          backoffMs = INITIAL_BACKOFF_MS
        } catch (err) {
          if (stopped) {
            break
          }
          reportError(err)
          await sleep(backoffMs)
          backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS)
        }
      }
      conn.disconnect()
    })()

    return {
      stop: async () => {
        stopped = true
        wake?.()
        // Aborts an in-flight blocking read so the loop exits promptly.
        conn.disconnect()
        await loop
      },
    }
  }
}
