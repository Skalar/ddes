import {AggregateCommit} from './EventStore'

/**
 * Yields batches of commits that can be safely processed in parallel.
 *
 * This means sequential processing of commits for a given aggregateType and aggregateKey
 * as well as respecting the dependency decisions via 'isDependent' option.
 */

export async function* parallelizableCommitBatches<TAggregateCommit extends AggregateCommit>(
  commits: AsyncIterable<TAggregateCommit[] | undefined>,
  params: Params = {}
): AsyncIterable<{commits: TAggregateCommit[]; cursor?: string}> {
  const {
    maxBacklogSize = 50,
    maxBatchSize = 50,
    isDependent = (a, b) =>
      a.aggregateType === b.aggregateType &&
      a.aggregateKey === b.aggregateKey &&
      a.aggregateVersion > b.aggregateVersion,
  } = params

  const backlog: Set<TAggregateCommit> = new Set()
  const batch: Set<TAggregateCommit> = new Set()
  let postponedCursor: string

  // Helper to check whether the batch contains a commit that the provided commit depends on
  const commitCanBePutInBatch = (commit: TAggregateCommit) => {
    for (const commitInBatch of batch) {
      if (isDependent(commit, commitInBatch)) {
        return false
      }
    }

    for (const commitInBacklog of backlog) {
      if (isDependent(commit, commitInBacklog)) {
        return false
      }
    }

    return true
  }

  const consumeBatch = () => {
    let progressCursor

    if (backlog.size) {
      // We have commits in the backlog, which means new cursor cannot be higher than theirs
      const earliestCursorInBacklog = Array.from(backlog).shift()!.chronologicalKey

      for (const commitInBatch of batch) {
        if (commitInBatch.chronologicalKey >= earliestCursorInBacklog) {
          if (!postponedCursor || commitInBatch.chronologicalKey > postponedCursor) {
            postponedCursor = commitInBatch.chronologicalKey
          }
          break
        }
        progressCursor = commitInBatch.chronologicalKey
      }

      if (
        postponedCursor &&
        postponedCursor < earliestCursorInBacklog &&
        (!progressCursor || postponedCursor > progressCursor)
      ) {
        progressCursor = postponedCursor
      }
    } else {
      if (batch.size) {
        // The cursor of the last commit in the batch is our new cursor
        progressCursor = Array.from(batch).pop()!.chronologicalKey
      }

      if (!progressCursor || postponedCursor > progressCursor) {
        progressCursor = postponedCursor
      }
    }

    const commits = Array.from(batch)
    batch.clear()

    return {commits, cursor: progressCursor}
  }

  for await (const commitBatch of commits) {
    if (commitBatch) {
      for (const commit of commitBatch) {
        if (batch.size < maxBatchSize && commitCanBePutInBatch(commit)) {
          batch.add(commit)
        } else {
          backlog.add(commit)
        }
      }
    }

    if (batch.size === maxBatchSize || (!commitBatch && batch.size)) {
      // We either have the desired batch size, or want to flush since we dont know when the next batch will come
      yield consumeBatch()
    }

    // We need to exhaust backlog, since we need to wait for further commits
    while (backlog.size > maxBacklogSize || (backlog.size && !commitBatch)) {
      for (const backlogCommit of backlog) {
        if (commitCanBePutInBatch(backlogCommit)) {
          batch.add(backlogCommit)
          backlog.delete(backlogCommit)
        }

        if (batch.size === maxBatchSize) break
      }

      if (batch.size) {
        yield consumeBatch()
      }
    }
  }
}

interface Params {
  // Maximum number of commits that will be included in a batch
  maxBatchSize?: number
  // Maximum number of commits "held back" to keep in memory while attempting to fill a batch
  maxBacklogSize?: number
  // Function that determines whether commitA is dependent on commitB
  isDependent?: (commitA: AggregateCommit, commitB: AggregateCommit) => boolean
}
