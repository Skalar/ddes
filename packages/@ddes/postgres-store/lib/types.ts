import {Client} from 'pg'

export interface Row {
  compositeId: string
  aggregateKey: string
  aggregateVersion: number
  aggregateType: string
  sortKey: string
  events: any[]
  partitionKey: string
  timestamp: number
  expiresAt?: number
}

export interface PostgresEventStoreConfig {
  tableName: string
  client: Client
  createdAt?: Date
}

/**
 * @hidden
 */
export interface PostgresStoreBatchMutatorQueueItem {
  startedPromise: Promise<any>
  startedResolver: () => void
  processedPromise: Promise<any>
  processedResolver: () => void
  processing: boolean
  query: string
  variables: any[]
}
