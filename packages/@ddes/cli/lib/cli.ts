/* eslint-disable no-console */
/**
 * @module @ddes/cli
 */

import {pickBy} from 'lodash'
import parseArgs from 'minimist'
import displayUsage from './displayUsage'
import loadCommands from './loadCommands'

export default async function cli() {
  const commandName = process.argv[2]
  const commands = await loadCommands()
  const matchingCommands = pickBy(commands, (v, k) => k.startsWith(commandName))
  let command = commands[commandName]

  if (!command && Object.keys(matchingCommands).length === 1) {
    command = commands[Object.keys(matchingCommands)[0]]
  }

  if (!command) {
    displayUsage(
      Object.keys(matchingCommands).length ? matchingCommands : commands
    )
  } else {
    try {
      const params = command.params(parseArgs(process.argv.slice(3)))
      await command.handler(params)
    } catch (error) {
      console.error(`Error: ${error.message}`)
      console.log('')
      console.log(`Usage:`)
      console.log(`ddes ${commandName} ${command.usage()}\n`)
    }
  }
}
