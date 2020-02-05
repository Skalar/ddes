/**
 * @module @ddes/cli
 */

// tslint:disable:no-var-requires

import findUp from 'find-up'
import {get} from 'lodash'
import {CliCommand} from './types'

/**
 * @hidden
 */
// eslint-disable-next-line @typescript-eslint/no-var-requires
const matchdep = require('matchdep')

/**
 * @hidden
 */
export default async function loadCommands() {
  const packageJsonPath = await findUp('package.json')

  const ddesPackages = matchdep.filterAll('@ddes/*', `${packageJsonPath}`)

  return ddesPackages.reduce(
    (col: {[commandName: string]: CliCommand}, pkgName: string) => ({
      ...col,
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      ...get(require(pkgName), 'cli.commands'),
    }),
    {}
  )
}
