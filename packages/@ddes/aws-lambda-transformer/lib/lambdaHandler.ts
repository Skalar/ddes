/**
 * @module @ddes/aws-lambda-transformer
 */

import {
  Transformation,
  TransformationWorkerResult,
} from '@ddes/store-transformations'
import {join} from 'path'

/**
 * @hidden
 */
export default async function lambdaHandler(
  input: {
    segment: number
    totalSegments: number
    readCapacityLimit: number
    writeCapacityLimit: number
    state: any
  },
  context: any
): Promise<TransformationWorkerResult> {
  if (!process.env.TRANSFORMATION_PATH) {
    throw new Error(`'TRANSFORMATION_PATH' env var not defined`)
  }

  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const transformationModule = require(join(
    process.cwd(),
    process.env.TRANSFORMATION_PATH
  ))

  const transformation = (transformationModule.default ||
    transformationModule) as Transformation

  return await transformation.perform({
    ...input,
    deadline: Date.now() + context.getRemainingTimeInMillis() - 10000,
  })
}
