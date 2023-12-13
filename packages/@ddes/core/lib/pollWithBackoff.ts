import { BackoffParams, jitteredBackoff } from './jitteredBackoff'
export type PollParams = BackoffParams

export function pollWithBackoff<TResult>(
	params: PollParams,
	pollFunction: () => AsyncIterable<TResult>,
): AsyncIterable<TResult | undefined> {
	const { minDelay, maxDelay, delayBackoffExponent } = params

	return {
		[Symbol.asyncIterator]() {
			let consecutiveEmptyPolls = 0
			let pollIterator: AsyncIterator<TResult> | undefined
			let delayTimer: NodeJS.Timeout | undefined

			return {
				async next() {
					while (true) {
						if (!pollIterator) {
							pollIterator = pollFunction()[Symbol.asyncIterator]()
						}

						const { value, done } = await pollIterator.next()

						if (done) {
							pollIterator = undefined
						}

						if (value) {
							consecutiveEmptyPolls = 0
							return { value, done: false }
						}
						consecutiveEmptyPolls++

						const delay = jitteredBackoff({
							minDelay,
							maxDelay,
							delayBackoffExponent,
							attempt: consecutiveEmptyPolls,
						})

						await new Promise((resolve) => {
							delayTimer = setTimeout(() => {
								delayTimer = undefined
								resolve(undefined)
							}, delay)
						})
					}
				},

				async return() {
					if (delayTimer) clearTimeout(delayTimer)
					if (typeof pollIterator !== 'undefined') await pollIterator.return?.()

					return { value: undefined, done: true }
				},

				async throw(...args) {
					await this.return?.()

					if (pollIterator?.throw) {
						return await pollIterator.throw(...args)
					}

					return { value: undefined, done: true }
				},
			}
		},
	}
}
