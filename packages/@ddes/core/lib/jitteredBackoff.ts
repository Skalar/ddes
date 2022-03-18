export interface BackoffParams {
  minDelay: number
  maxDelay: number
  delayBackoffExponent?: number
}

export function randomIntInRange(min: number, max: number) {
  return Math.floor(Math.random() * (max - min + 1) + min)
}

export function jitteredBackoff(
  params: BackoffParams & {
    attempt: number
  }
) {
  const {minDelay, maxDelay, delayBackoffExponent = 1.5, attempt} = params
  const delay = Math.max(
    params.minDelay,
    Math.min(randomIntInRange(0, minDelay * delayBackoffExponent ** attempt), maxDelay)
  )
  // console.dir({attempt, delay})
  return delay
}
