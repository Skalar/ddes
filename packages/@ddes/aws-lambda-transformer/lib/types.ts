/**
 * @module @ddes/aws-lambda-transformer
 */

import {ConfigurationOptions} from 'aws-sdk/lib/config'

export interface LambdaTransformerConfig {
  fileRoot?: string
  workerCount?: number
  files?: string[]
  ignoreFiles?: string[]
  awsConfig?: ConfigurationOptions
  memorySize?: number
  timeout?: number
  environment?: {
    [key: string]: string
  }
}
