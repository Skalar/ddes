#!/usr/bin/env ts-node-script

import {Account, AccountState} from '../aggregateRoots/Account'
import {AggregateInstanceData, retryOnVersionConflict} from '@ddes/core'
import {dispose} from '../support/disposables'

export async function depositMoney(companyId: string, accountId: string, amount: number) {
  if (amount <= 0) throw new Error(`Amount must be a positive integer`)

  let account: AggregateInstanceData<AccountState> | undefined = undefined

  await retryOnVersionConflict(async () => {
    account = await Account.get({companyId, accountId}, undefined, account)

    if (!account) throw new Error(`Account not found`)

    await Account.commit({companyId, accountId}, account.version + 1, [
      Account.events.MoneyDeposited(amount),
    ])
  })
}

async function main() {
  await depositMoney(process.argv[2], process.argv[3], parseInt(process.argv[4], 10))
  await dispose()
}

if (require.main === module) main()
