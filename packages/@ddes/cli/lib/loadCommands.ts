/**
 * @module @ddes/cli
 */

// tslint:disable:no-var-requires

import * as findUp from 'find-up'
import {get} from 'lodash'
import {CliCommand} from './types'

/**
 * @hidden
 */
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
      ...get(require(pkgName), 'cli.commands'),
    }),
    {}
  )
}
