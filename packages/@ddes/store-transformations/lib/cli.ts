/**
 * @module @ddes/store-transformations
 */

import {join} from 'path'
import Transformer from './Transformer'

/**
 * Commands are normally invoked via `ddes <command> [..args]`
 */
const cli = {
  commands: {
    'transform:local': {
      description: 'Transform store with local workers',

      usage() {
        return '<path-to-transformation-file> [--transformerOption=value] ...'
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

        // Load at runtime to avoid dependency on cli
        const TransformerGui = require('@ddes/store-transformations/lib/TransformerGui')
          .default

        const transformationModule = require(join(
          process.cwd(),
          transformationPath
        ))

        const transformer = new Transformer(
          transformationModule.default || transformationModule,
          transformerOptions
        )

        const gui = new TransformerGui(transformer, () => process.exit(0))

        try {
          await transformer.execute()
          console.log('Transformation completed successfully.')
        } finally {
          gui.terminate()
        }
      },
    },
  },
}

export default cli
