/**
 * @module @ddes/cli
 */

// tslint:disable:no-var-requires

import chalk from 'chalk'
import {pointer} from 'figures'
import findUp from 'find-up'
import {CliCommand} from './types'

/**
 * @hidden
 */
// eslint-disable-next-line @typescript-eslint/no-var-requires
const columnify = require('columnify')

/**
 * @hidden
 */
// eslint-disable-next-line @typescript-eslint/no-var-requires
const boxen = require('boxen')

export default async function displayUsage(commands: {
  [commandName: string]: CliCommand
}) {
  const packageJsonPath = await findUp('package.json', {cwd: __dirname})
  const version = require(packageJsonPath!).version
  console.log(
    boxen(
      `${chalk.blueBright(pointer)} ${chalk.bold.blueBright(
        'DDES CLI'
      )} ${chalk.white(`v${version}`)}`,
      {
        borderColor: 'white',
        padding: {left: 8, top: 1, bottom: 1, right: 12},
      }
    )
  )

  console.log(
    `\n${chalk.greenBright.bold('$')} ${chalk.bold.blue(
      'ddes'
    )} <command> [...args]\n`
  )

  if (Object.keys(commands).length) {
    const commandTable = columnify(
      Object.keys(commands)
        .sort()
        .map(commandName => ({
          command: chalk.blueBright(commandName),
          description: commands[commandName].description,
        })),
      {
        headingTransform(heading: string) {
          return chalk.yellow(heading.toUpperCase())
        },
        columnSplitter: '   ',
      }
    )

    console.log(`${commandTable}\n`)
  }
}
