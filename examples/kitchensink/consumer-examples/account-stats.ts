#!/usr/bin/env ts-node-script

import {pollEventStore} from '@ddes/core'
import {store} from '../config'
import * as aggregateRoots from '../aggregateRoots'

async function main() {
  let totalActiveAccounts = 0
  let totalBalance = 0

  function updateStatsDisplay() {
    process.stdout.clearLine(0)
    process.stdout.cursorTo(0)
    process.stdout.write(
      `Active accounts: ${totalActiveAccounts} | Total balance: ${totalBalance} `
    )
  }

  updateStatsDisplay()

  aggregateRoots.Account
  const initialCursor = new Date(Date.now() - 1000 * 60 * 60 * 24 * 7) // A week ago

  for await (const commit of pollEventStore({
    store,
    initialCursor,
    aggregateRoots,
  })) {
    if (!commit) continue

    switch (commit.aggregateType) {
      case 'Account': {
        for (const event of commit.events) {
          switch (event.type) {
            case 'AccountOpened':
              totalActiveAccounts += 1
              break
            case 'AccountClosed':
              totalActiveAccounts -= 1
              break
            case 'MoneyDeposited':
              totalBalance += event.amount
              break
            case 'MoneyWithdrawn':
              totalBalance -= event.amount
              break
          }
          break
        }
      }
    }

    updateStatsDisplay()
  }
}

if (require.main === module) main()
