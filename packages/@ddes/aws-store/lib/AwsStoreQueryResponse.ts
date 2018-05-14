/**
 * @module @ddes/aws-store
 */

import {
  Commit,
  Event,
  StoreQueryResponse,
  StoreQueryResultSet,
} from '@ddes/core'
import {DynamoDB} from 'aws-sdk'
import AwsStore from './AwsStore'
import {MarshalledCommit} from './types'
import {unmarshallCommit} from './utils'

export default class AwsStoreQueryResponse implements StoreQueryResponse {
  private responseIterator: AsyncIterableIterator<
    DynamoDB.QueryOutput & {
      throttleCount: number
    }
  >

  private store: AwsStore

  constructor(
    store: AwsStore,
    responseIterator: AsyncIterableIterator<
      DynamoDB.QueryOutput & {throttleCount: number}
    >
  ) {
    this.responseIterator = responseIterator
    this.store = store
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
        throttleCount: rawQueryResult.throttleCount,
        items: rawQueryResult.Items,
        scannedCount: rawQueryResult.ScannedCount,
        ...(rawQueryResult.ConsumedCapacity && {
          consumedCapacity: {
            table: rawQueryResult.ConsumedCapacity.Table
              ? rawQueryResult.ConsumedCapacity.Table.CapacityUnits
              : 0,
            chronological:
              rawQueryResult.ConsumedCapacity.GlobalSecondaryIndexes &&
              rawQueryResult.ConsumedCapacity.GlobalSecondaryIndexes
                .chronological!.CapacityUnits,
          },
        }),

        get commits() {
          return self.commitsFromQueryOutput(rawQueryResult)
        },
      } as StoreQueryResultSet
    }
  }

  protected async *commitsFromQueryOutput(queryOutput: DynamoDB.QueryOutput) {
    if (!queryOutput.Items) {
      return
    }

    for (const item of queryOutput.Items) {
      yield await unmarshallCommit(item as MarshalledCommit)
    }
  }
}
