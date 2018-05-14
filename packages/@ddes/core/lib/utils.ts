/**
 * @module @ddes/core
 *
 */

import {RetryConfig, Timestamp} from './types'

/**
 * @hidden
 */
function randomIntInRange(min: number, max: number) {
  return Math.floor(Math.random() * (max - min + 1) + min)
}

/**
 * @hidden
 */
export function jitteredBackoff(params: {
  initialValue: number
  maxValue: number
  backoffExponent: number
  attempt: number
}) {
  if (params.attempt === 0) {
    return params.initialValue
  }

  return Math.min(
    params.maxValue,
    randomIntInRange(
      0,
      params.initialValue * params.backoffExponent ** params.attempt
    )
  )
}

/**
 * @hidden
 */
export function toTimestamp(
  obj: string | Date | undefined | number
): Timestamp {
  if (!obj) {
    return new Date().valueOf()
  } else if (obj instanceof Date) {
    return obj.valueOf()
  } else if (typeof obj === 'number') {
    return obj
  } else {
    return new Date(obj as string).valueOf()
  }
}

/**
 * @hidden
 */
export async function jitteredRetry(
  fn: () => Promise<any>,
  options: RetryConfig
) {
  const startedAt = Date.now()
  let attempt = 0

  while (Date.now() - startedAt < options.timeout) {
    attempt++

    try {
      return await fn()
    } catch (error) {
      if (!options.errorIsRetryable(error)) {
        throw error
      }

      const delay = jitteredBackoff({
        initialValue: options.initialDelay,
        maxValue: options.maxDelay,
        backoffExponent: options.backoffExponent || 2,
        attempt,
      })

      const timeleft = startedAt + options.timeout - Date.now()

      await new Promise(resolve =>
        setTimeout(resolve, Math.min(delay, timeleft))
      )

      if (options.beforeRetry) {
        await options.beforeRetry()
      }
    }
  }
}
