import {ConnectionPoolConfig, SQLQuery} from '@databases/pg'

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

export interface PostgresStoreConfig {
  tableName: string
  database: string | ConnectionPoolConfig
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
  query: SQLQuery
}
