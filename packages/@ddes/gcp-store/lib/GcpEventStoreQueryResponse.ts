import {
  Commit,
  Event,
  StoreQueryResponse,
  StoreQueryResultSet,
} from '@ddes/core'
import {MarshalledCommit} from './types'
import {unmarshallCommit} from './utils'

/**
 * @module @ddes/gcp-store
 */

export default class GcpEventStoreQueryResponse implements StoreQueryResponse {
  private responseIterator: AsyncIterableIterator<MarshalledCommit>

  constructor(responseIterator: AsyncIterableIterator<MarshalledCommit>) {
    this.responseIterator = responseIterator
  }

  /**
   * @hidden
   */
  public [Symbol.asyncIterator]() {
    return this.asResultSets()
  }

  public get commits() {
    return this.asCommits()
  }

  public get events() {
    return this.asEvents()
  }

  protected async *asEvents(): AsyncIterableIterator<Event> {
    for await (const commit of this.asCommits()) {
      for (const event of commit.events) {
        yield event
      }
    }
  }

  protected async *asCommits(): AsyncIterableIterator<Commit> {
    for await (const resultSet of this.asResultSets()) {
      for await (const commit of resultSet.commits) {
        yield commit
      }
    }
  }

  protected async *asResultSets() {
    const self = this

    for await (const rawQueryResult of this.responseIterator) {
      yield {
        get commits() {
          return self.commitsFromQueryOuput(rawQueryResult)
        },
      } as StoreQueryResultSet
    }
  }

  protected async *commitsFromQueryOuput(queryOutput: MarshalledCommit) {
    if (!queryOutput) {
      return
    }

    yield await unmarshallCommit(queryOutput as MarshalledCommit)
  }
}
