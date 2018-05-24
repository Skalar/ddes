/**
 * @module @ddes/aws-lambda-transformer
 */

import {join} from 'path'
import LambdaTransformer from './LambdaTransformer'

/**
 * Commands are normally invoked via `ddes <command> [..args]`
 */
const cli = {
  commands: {
    'transform:lambda': {
      description: 'Transform EventStore using AWS lambda workers',

      usage() {
        return '<path-to-transformation-file> [--transformerOption=value]'
      },

      params(args: {_: string[]} & {workerCount: number}) {
        const {
          _: [transformationPath],
          ...transformerOptions
        } = args

        if (!transformationPath) {
          throw new Error('path to transformation file must be specified')
        }

        return {transformationPath, transformerOptions}
      },

      async handler(params: {
        transformationPath: string
        transformerOptions: any
      }) {
        const {transformationPath, transformerOptions} = params

        // Load at runtime to avoid dependencies in lambda package
        const TransformerGui = require('@ddes/store-transformations/gui')
          .default

        const transformationModule = require(join(
          process.cwd(),
          transformationPath
        ))

        const transformer = new LambdaTransformer(
          transformationModule.default || transformationModule,
          transformationPath,
          transformerOptions
        )
        const gui = new TransformerGui(transformer)

        await transformer.execute()
      },
    },
  },
}

export default cli
