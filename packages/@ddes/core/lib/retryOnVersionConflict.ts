import {VersionConflictError} from './EventStore'
import {jitteredBackoff} from './jitteredBackoff'
import {PollParams} from './pollWithBackoff'

export async function retryOnVersionConflict<TReturnType>(
  fn: () => Promise<TReturnType>,
  options: {
    timeout?: number
  } & Partial<PollParams> = {}
) {
  const {timeout = 2000, minDelay = 2, maxDelay = 500, delayBackoffExponent = 2} = options

  const startedAt = Date.now()
  let attempt = 0
  let lastError: VersionConflictError

  while (Date.now() - startedAt < timeout) {
    attempt++

    try {
      return await fn()
    } catch (error: any) {
      if (error.toString().includes('VersionConflictError')) {
        lastError = error
      } else {
        throw error
      }

      const delay = jitteredBackoff({
        minDelay,
        maxDelay,
        delayBackoffExponent,
        attempt,
      })

      const timeleft = startedAt + timeout - Date.now()

      await new Promise(resolve => setTimeout(resolve, Math.min(delay, timeleft)))
    }
  }

  const versionConflictError = new VersionConflictRetryError(lastError!, attempt)
  throw versionConflictError
}

export class VersionConflictRetryError extends Error {
  public readonly lastError: VersionConflictError
  public readonly attempts: number

  constructor(lastError: VersionConflictError, attempts: number) {
    const {
      commit: {aggregateVersion, aggregateType, aggregateKey},
    } = lastError
    super(
      `Gave up committing version ${aggregateVersion} of ${aggregateType}<${aggregateKey}> after ${attempts} attempts`
    )
    Object.setPrototypeOf(this, new.target.prototype)
    this.name = 'VersionConflictRetryError'
    this.lastError = lastError
    this.attempts = attempts
  }
}
