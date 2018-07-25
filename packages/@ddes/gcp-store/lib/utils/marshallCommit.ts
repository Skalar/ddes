/**
 * @module @ddes/gcp-store
 */

import {Commit} from '@ddes/core'
import {promisify} from 'util'
import {gzip as gzipCb} from 'zlib'
import {MarshalledCommitProperty} from '../types'

/**
 * @hidden
 */

const gzip = promisify(gzipCb)

/**
 * @hidden
 */
export default async function marshallCommit(
  commit: Commit
): Promise<MarshalledCommitProperty[]> {
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

  const zippedEvents = (await gzip(
    JSON.stringify(
      events.map(({type: t, version: v, properties: p}) => ({
        ...(v && {v}),
        p,
        t,
      }))
    )
  )) as string

  return [
    {
      name: 's',
      value: [aggregateType, aggregateKey].join(':'),
    },
    {
      name: 'v',
      value: aggregateVersion,
    },
    {
      name: 'g',
      value: sortKey,
    },
    {
      name: 'a',
      value: aggregateType,
    },
    {
      name: 'r',
      value: aggregateVersion === 1 ? aggregateKey : null,
    },
    {
      name: 't',
      value: new Date(timestamp).valueOf(),
    },
    {
      name: 'e',
      value: zippedEvents,
      excludeFromIndexes: true,
    },
    {
      name: 'x',
      value: expiresAt || '',
    },
    {
      name: 'p',
      value: `${new Date(timestamp)
        .toISOString()
        .split('T')[0]
        .replace(/\-/g, '')}${chronologicalGroup}`,
    },
  ] as MarshalledCommitProperty[]
}
