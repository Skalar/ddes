/**
 * @module @ddes/aws-lambda-transformer
 */

import {
  Transformation,
  Transformer,
  TransformerState as State,
} from '@ddes/store-transformations'
import {AWSError, Lambda, Request} from 'aws-sdk'
import {ConfigurationOptions} from 'aws-sdk/lib/config-base'
import debug from 'debug'
import * as lambdaFunction from './lambdaFunction'
import {LambdaTransformerConfig} from './types'

/**
 * @hidden
 */
const log = debug('@ddes/aws-store-lambda-transformer:LambdaTransformer')

/**
 * Used to execute [[Transformation]]s using AWS Lambda workers.
 *
 * ```typescript
 * const transformer = new LamdaTransformer(
 *   transformation,
 *   {
 *     workerCount: 4,
 *     readCapacityLimit: 100,
 *     writeCapacityLimit: 300,
 *   }
 * )
 *
 * await transformer.execute()
 * ```
 */
export default class LambdaTransformer extends Transformer {
  private activeRequests: Set<
    Request<Lambda.InvocationResponse, AWSError>
  > = new Set()

  private awsConfig?: ConfigurationOptions
  private config: LambdaTransformerConfig
  private transformationPath: string

  constructor(
    transformation: Transformation,
    transformationPath: string,
    options: LambdaTransformerConfig
  ) {
    super(transformation, options)
    this.transformationPath = transformationPath
    this.config = Object.assign(
      {},
      this.transformation.transformerConfig.lambda,
      options
    ) as LambdaTransformerConfig
  }

  public async terminate(options: {skipCleanup?: boolean} = {}) {
    await super.terminate()

    for (const request of this.activeRequests) {
      request.abort()
    }

    if (options.skipCleanup) {
      return
    }

    await lambdaFunction.clean(this.runId, this.config)
  }

  public async setup() {
    await Promise.all([super.setup(), this.deployFunction()])
  }

  protected async deployFunction() {
    try {
      await lambdaFunction.deploy(
        this.runId,
        this.transformationPath,
        this.config
      )
    } catch (error) {
      await lambdaFunction.clean(this.runId, this.config)
      throw error
    }
  }

  get statusDescription() {
    switch (this.state) {
      case State.Running:
        return `running (${this.activeWorkers} lambda workers${
          this.config.memorySize ? `[${this.config.memorySize} MiB]` : ''
        })`
      default:
        return this.state
    }
  }

  protected async workerLoop(index: number) {
    const lambda = new Lambda(this.config.awsConfig)

    while (this.state === State.Running) {
      const request = await lambda.invoke({
        FunctionName: this.runId,
        InvocationType: 'RequestResponse',
        LogType: 'Tail',
        Payload: JSON.stringify({
          segment: index,
          totalSegments: this.workerCount,
          readCapacityLimit: this.readCapacityLimit
            ? Math.floor(this.readCapacityLimit / this.workerCount)
            : undefined,
          writeCapacityLimit: this.writeCapacityLimit
            ? Math.floor(this.writeCapacityLimit / this.workerCount)
            : undefined,
        }),
      })
      this.activeRequests.add(request)

      try {
        log(`awaiting request promise ${index}`)
        const workerResult = await request.promise()
        log(`workerResult ${index}`, workerResult)
        const {StatusCode, FunctionError, LogResult, Payload} = workerResult
        const logResult = Buffer.from(LogResult!, 'base64').toString('utf8')

        if (StatusCode !== 200) {
          throw workerResult.FunctionError
        }
        if (FunctionError) {
          throw new Error(`FunctionError ${FunctionError}`)
        }
        if (!Payload) {
          throw new Error('Worker completed without a payload')
        }
        const {completed, state: newState, ...counters} = JSON.parse(
          Payload as string
        )

        this.bumpCounters(counters as any)

        if (newState) {
          await this.updateWorkerState(index, newState)
        }

        if (completed) {
          this.activeWorkers--
          return
        }
      } catch (error) {
        if (
          error.code === 'AccessDeniedException' &&
          error.message ===
            'The role defined for the function cannot be assumed by Lambda.'
        ) {
          await new Promise(r => setTimeout(r, 1000))
        } else if (error.code === 'ServiceException') {
          await new Promise(r => setTimeout(r, 500))
        } else if (error.code === 'RequestAbortedError') {
          return
        } else {
          throw error
        }
      } finally {
        this.activeRequests.delete(request)
      }
    }
  }
}
