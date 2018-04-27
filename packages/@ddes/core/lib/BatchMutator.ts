/**
 * @module @ddes/core
 */

import {Commit} from './Commit'
import {CommitOrCommits} from './types'

export abstract class BatchMutator {
  public drained?: Promise<void>

  public asIterable(commits: CommitOrCommits): Iterable<Commit> {
    if (Array.isArray(commits)) {
      return commits
    } else if ((commits as any).next) {
      return commits as Iterable<Commit>
    } else {
      return [commits] as Commit[]
    }
  }

  public abstract put(commits: CommitOrCommits): Promise<void>
  public abstract delete(commits: CommitOrCommits): Promise<void>
}
