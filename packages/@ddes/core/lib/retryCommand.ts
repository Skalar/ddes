/**
 * @module @ddes/core
 */

import {Aggregate} from './Aggregate'
import {VersionConflictError} from './errors'
import {JitteredRetryOptions} from './types'
import {jitteredRetry} from './utils'

/**
 *
 * Decorator for Aggregate methods (commands)
 *
 * By default retries on VersionConflictError
 */
export function retryCommand(options?: Partial<JitteredRetryOptions>) {
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
