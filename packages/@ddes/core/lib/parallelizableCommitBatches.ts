import {AggregateCommit} from './EventStore'

/**
 * Yields batches of commits that can be safely processed in parallel.
 *
 * This means sequential processing of commits for a given aggregateType and aggregateKey
 * as well as respecting the dependency decisions via 'isDependent' option.
 */
export async function* parallelizableCommitBatches(
  commits: AsyncIterable<AggregateCommit | undefined>,
  params: {
    // Maximum number of commits that will be included in a batch
    maxBatchSize?: number
    // Maximum number of commits "held back" to keep in memory while attempting to fill a batch
    maxBacklogSize?: number
    // Function that determines whether commitA is dependent on commitB
    isDependent?: (commitA: AggregateCommit, commitB: AggregateCommit) => boolean
    // Promise that when resolved will abort iterator
    aborted?: Promise<any>
  }
) {
  const {maxBacklogSize = 1000, maxBatchSize = 100, aborted: abortedPromise} = params

  let aborted = false
  abortedPromise?.then(() => (aborted = true))

  let backlog: Set<AggregateCommit> = new Set()
  let batch: Set<AggregateCommit> = new Set()

  // Helper to check whether the batch contains a commit that the provided commit depends on
  const commitCanBePutInBatch = (commit: AggregateCommit) => {
    for (const commitInBatch of batch) {
      if (
        (commit.aggregateType === commitInBatch.aggregateType && commit.aggregateKey === commitInBatch.aggregateKey) ||
        (params.isDependent && params.isDependent(commit, commitInBatch))
      ) {
        return false
      }
    }

    return true
  }

  // Use the provided commit iterator, initially
  let commitIterator: Iterable<AggregateCommit> | AsyncIterable<AggregateCommit | undefined> = commits

  while (!aborted) {
    /**
     * Move as many backlog commits as possible to the batch
     */
    for (const backlogCommit of backlog) {
      if (commitCanBePutInBatch(backlogCommit)) {
        batch.add(backlogCommit)
        backlog.delete(backlogCommit)
      }

      if (batch.size === maxBatchSize) break
    }

    /**
     * Fill batch as much as possible with new commits
     */
    let exhaustedCommitIterator = false

    if (batch.size < maxBatchSize) {
      exhaustedCommitIterator = true
      for await (const commit of commitIterator) {
        if (aborted) return

        if (commit) {
          if (commitCanBePutInBatch(commit)) {
            batch.add(commit)
          } else {
            backlog.add(commit)
          }
        }

        if (
          (!commit && batch.size) || //
          batch.size === maxBatchSize ||
          backlog.size === maxBacklogSize
        ) {
          // We do not want to wait for more commits
          exhaustedCommitIterator = false
          break
        }
      }
    }

    /**
     * Yield batch and progress cursor, we either reached max batch size or commit iterator was exhausted
     */
    if (batch.size) {
      // Figure out new progress cursor
      let progressCursor

      if (backlog.size) {
        // We have commits in the backlog, which means new cursor cannot be higher than theirs
        const earliestCursorInBacklog = Array.from(backlog).shift()!.chronologicalKey

        for (const commitInBatch of batch) {
          if (commitInBatch.chronologicalKey >= earliestCursorInBacklog) break
          progressCursor = commitInBatch.chronologicalKey
        }
      } else {
        if (batch.size) {
          // The cursor of the last commit in the batch is our new cursor
          progressCursor = Array.from(batch).pop()!.chronologicalKey
        }
      }

      yield {commits: Array.from(batch), progressCursor}
      batch = new Set()
    }

    if (exhaustedCommitIterator) {
      if (backlog.size) {
        // Repeat using current backlog as commit iterator
        commitIterator = backlog
        backlog = new Set()
      } else {
        // We have consumed all the commits there are
        return
      }
    }

    // Still commits left to consume
  }
}
