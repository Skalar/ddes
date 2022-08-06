#!/usr/bin/env ts-node-script

import {Account} from '../aggregateRoots/Account'

async function main(companyId: string, accountId: string, minVersion: number) {
  const commitsIterator = Account.streamCommits({companyId, accountId}, minVersion)

  for await (const commits of commitsIterator) {
    console.dir({commits})
  }
}

if (require.main === module)
  main(process.argv[2], process.argv[3], process.argv[4] ? parseInt(process.argv[4], 10) : 1)
