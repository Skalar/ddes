#!/usr/bin/env ts-node-script

import {Account, AccountState} from '../aggregateRoots/Account'
import {AggregateInstanceData, retryOnVersionConflict} from '@ddes/core'
import {dispose} from '../support/disposables'

export async function openAccount(companyId: string, accountId: string) {
  let account: AggregateInstanceData<AccountState> | undefined = undefined

  await retryOnVersionConflict(async () => {
    account = await Account.get({companyId, accountId}, undefined, account)

    if (!account) throw new Error(`Account not found`)
    if (account.state.balance > 0)
      throw new Error(`Balance of ${account.state.balance} must be cleared about before closure`)

    await Account.commit({companyId, accountId}, account.version + 1, [
      Account.events.AccountClosed(),
    ])
  })
}

async function main() {
  await openAccount(process.argv[2], process.argv[3])
  await dispose()
}

if (require.main === module) main()
