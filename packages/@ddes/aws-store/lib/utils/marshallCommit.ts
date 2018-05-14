/**
 * @module @ddes/aws-store
 */

import {Commit} from '@ddes/core'
import {DynamoDB} from 'aws-sdk'
import {promisify} from 'util'
import {gzip as gzipCb} from 'zlib'
import {MarshalledCommit} from '../types'

/**
 * @hidden
 */
const gzip = promisify(gzipCb)

/**
 * @hidden
 */
export default async function marshallCommit(
  commit: Commit
): Promise<MarshalledCommit> {
  const {
    aggregateType,
    aggregateKey,
    sortKey,
    events,
    timestamp,
    aggregateVersion,
    expiresAt,
    chronologicalGroup,
  } = commit

  return DynamoDB.Converter.marshall({
    s: [aggregateType, aggregateKey].join(':'),
    v: aggregateVersion,
    g: sortKey,
    a: aggregateType,
    r: aggregateVersion === 1 ? aggregateKey : undefined,
    t: new Date(timestamp).valueOf(),
    e: await gzip(
      JSON.stringify(
        events.map(({type: t, version: v, properties: p}) => ({
          ...(v && {v}),
          p,
          t,
        }))
      )
    ),
    x: expiresAt,
    p: `${new Date(timestamp)
      .toISOString()
      .split('T')[0]
      .replace(/\-/g, '')}${chronologicalGroup}`,
  }) as MarshalledCommit
}
