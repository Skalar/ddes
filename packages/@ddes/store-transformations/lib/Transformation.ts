/**
 * @module @ddes/store-transformations
 */

import {EventStore} from '@ddes/core'
import {TransformationWorkerInput, TransformationWorkerResult} from './types'

/**
 * Base transformation class, can be used/extended for custom [[EventStore]] transformations.
 * Is most useful when used in conjunction with a [[Transformer]].
 *
 * ```typescript
 * export default new Transformation({
 *   name: 'My custom transformation',
 *
 *   source: new AwsEventStore({tableName: 'tableA'}),
 *   target: new AwsEventStore({tableName: 'tableB'}),
 *
 *   async perform(input) {
 *     // do work within deadline
 *     return result
 *   }
 * })
 * ```
 */
class Transformation {
  public name: string
  public source: EventStore
  public target: EventStore
  public transformerConfig?: any

  constructor(transformationSpec: {
    name: string
    source: EventStore
    target: EventStore
    perform?: Transformation['perform']
    transformerConfig?: any
  }) {
    const {
      name,
      source,
      target,
      perform,
      transformerConfig,
    } = transformationSpec
    this.name = name
    this.source = source
    this.target = target
    this.transformerConfig = transformerConfig
    if (perform) {
      this.perform = perform
    }
  }

  public async perform(
    _input: TransformationWorkerInput
  ): Promise<TransformationWorkerResult> {
    throw new Error(`perform() must be specified in transformation`)
  }

  get isInPlaceTransformation() {
    return this.target === this.source
  }
}

export default Transformation
