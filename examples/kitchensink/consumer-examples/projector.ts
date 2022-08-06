#!/usr/bin/env ts-node-script

import {parallelizableCommitBatches} from '@ddes/core'
import {AccountCommit} from '../aggregateRoots/Account'
import {postgres} from '../stores'

/**
 * Stream batches of commits that can be safely processed in parallel
 */
async function main() {
  const commitsStream = postgres.streamCommits<AccountCommit>({}, true)
  for await (const {commits, cursor: newCursor} of parallelizableCommitBatches(commitsStream)) {
    console.dir({processCommits: commits}, {depth: null})

    if (newCursor) {
      console.log(`new cursor: '${newCursor}'`)
    }
  }
}

if (require.main === module) main()
