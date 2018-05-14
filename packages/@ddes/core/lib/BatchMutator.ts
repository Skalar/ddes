/**
 * @module @ddes/core
 */

import Commit from './Commit'
import {MarshalledCommit} from './types'

export default abstract class BatchMutator<T = MarshalledCommit> {
  public writeCount: number = 0
  public deleteCount: number = 0
  public throttleCount: number = 0
  public drained?: Promise<void>
  public abstract put(commits: Array<Commit | T> | Commit | T): Promise<void>
  public abstract delete(commits: Array<Commit | T> | Commit | T): Promise<void>

  protected asIterable(
    commits: Array<Commit | T> | Commit | T
  ): Iterable<Commit | T> {
    if (Array.isArray(commits)) {
      return commits
    } else if ((commits as any).next) {
      return (commits as any) as Iterable<Commit | T>
    } else {
      return [commits] as Array<Commit | T>
    }
  }
}
