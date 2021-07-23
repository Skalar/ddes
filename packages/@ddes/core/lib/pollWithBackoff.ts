import {BackoffParams, jitteredBackoff} from './jitteredBackoff'

export interface PollParams extends BackoffParams {
  aborted?: Promise<any>
}

export async function* pollWithBackoff<TResult>(
  params: PollParams,
  pollFunction: () => AsyncIterable<TResult>
): AsyncGenerator<TResult | undefined> {
  const {minDelay, maxDelay, delayBackoffExponent, aborted: abortedPromise} = params

  let aborted = false
  let pollDelayPromiseResolver: undefined | ((value?: unknown) => void)

  abortedPromise?.then(() => {
    aborted = true
    if (pollDelayPromiseResolver) {
      pollDelayPromiseResolver()
    }
  })

  let consecutiveEmptyPolls = 0

  while (!aborted) {
    let pollResultCount = 0

    for await (const pollResult of pollFunction()) {
      pollResultCount++
      yield pollResult
    }

    if (pollResultCount === 0) {
      consecutiveEmptyPolls++

      yield

      const delay = jitteredBackoff({
        minDelay,
        maxDelay,
        delayBackoffExponent,
        attempt: consecutiveEmptyPolls,
      })

      await new Promise(resolve => {
        pollDelayPromiseResolver = resolve
        setTimeout(() => {
          resolve(undefined)
          pollDelayPromiseResolver = undefined
        }, delay)
      })
    } else {
      consecutiveEmptyPolls = 0
    }
  }
}
