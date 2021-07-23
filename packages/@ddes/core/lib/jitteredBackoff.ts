export interface BackoffParams {
  minDelay: number
  maxDelay: number
  delayBackoffExponent: number
}

export function randomIntInRange(min: number, max: number) {
  return Math.floor(Math.random() * (max - min + 1) + min)
}

export function jitteredBackoff(
  params: BackoffParams & {
    attempt: number
  }
) {
  const {minDelay, maxDelay, delayBackoffExponent, attempt} = params

  return Math.min(
    params.minDelay,
    randomIntInRange(0, Math.min(minDelay * delayBackoffExponent ** attempt, maxDelay))
  )
}
