import {StoreQueryResponse, StoreQueryResultSet, Event, Commit} from '@ddes/core'
import {rowToCommit} from './utils'

export default class PostgresEventStoreQueryResponse implements StoreQueryResponse {
  constructor(private responseIterator: AsyncIterableIterator<any>) {}

  public get commits() {
    return this.asCommits()
  }

  public get events() {
    return this.asEvents()
  }

  /**
   * @hidden
   */
  public [Symbol.asyncIterator]() {
    return this.asResultSet()
  }

  protected async *asEvents(): AsyncIterableIterator<Event> {
    for await (const commit of this.asCommits()) {
      for (const event of commit.events) {
        yield event
      }
    }
  }
  protected async *asCommits(): AsyncIterableIterator<Commit> {
    for await (const resultSet of this.asResultSet()) {
      for await (const commit of resultSet.commits) {
        yield commit
      }
    }
  }

  protected async *asResultSet() {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const self = this

    for await (const row of this.responseIterator) {
      yield {
        items: [row],

        get commits() {
          return self.commitsFromOutput(row)
        },
      } as StoreQueryResultSet
    }
  }

  protected async *commitsFromOutput(row: any) {
    yield rowToCommit(row)
  }
}
