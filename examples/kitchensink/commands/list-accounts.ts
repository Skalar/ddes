#!/usr/bin/env ts-node-script

import {Account} from '../aggregateRoots/Account'
import {dispose} from '../support/disposables'

export async function listAccounts() {
  for await (const account of Account.scanInstances()) {
    console.log(`${account.key}`)
  }
}

async function main() {
  await listAccounts()
  await dispose()
}

if (require.main === module) main()
