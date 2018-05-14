/**
 * @module @ddes/core
 */

import Aggregate from './Aggregate'
import {VersionConflictError} from './errors'
import {RetryConfig} from './types'
import {jitteredRetry} from './utils'

/**
 *
 * Decorator for Aggregate methods (commands)
 *
 * By default retries on VersionConflictError
 */
export default function retryCommand(options?: Partial<RetryConfig>) {
  return (
    target: Aggregate,
    propertyKey: string,
    descriptor: PropertyDescriptor
  ) => {
    const method = descriptor.value

    descriptor.value = async function(this: Aggregate, ...commandArgs: any[]) {
      return await jitteredRetry(() => method!.apply(this, commandArgs), {
        errorIsRetryable: error => error instanceof VersionConflictError,
        beforeRetry: () => this.hydrate(),
        ...(target.constructor as typeof Aggregate).defaultRetryOptions,
        ...options,
      })
    }
  }
}
