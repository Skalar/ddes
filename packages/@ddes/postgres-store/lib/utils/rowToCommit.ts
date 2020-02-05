import {Commit} from '@ddes/core'

interface CommitRow {
  composite_id: string
  aggregate_key: string
  aggregate_version: string
  aggregate_type: string
  sort_key: string
  events: any[]
  partition_key: string
  chronological_group: string
  timestamp: string
  expires_at?: string | null
}

export default function ({
  composite_id,
  aggregate_key,
  aggregate_version,
  aggregate_type,
  sort_key,
  partition_key,
  chronological_group,
  expires_at,
  timestamp,
  ...rest
}: CommitRow): Commit {
  return new Commit({
    aggregateKey: aggregate_key,
    aggregateVersion: parseInt(aggregate_version, 10),
    aggregateType: aggregate_type,
    sortKey: sort_key,
    chronologicalGroup: chronological_group,
    ...(expires_at ? {expiresAt: parseInt(expires_at)} : {}),
    ...(timestamp ? {timestamp: parseInt(timestamp)} : {}),
    ...rest,
  })
}
