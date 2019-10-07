/**
 * @module @ddes/gcp-store
 */

import {Commit} from '@ddes/core'
import {promisify} from 'util'
import {gzip as gzipCb} from 'zlib'
import {MarshalledCommit, MarshalledCommitProperty} from '../types'

/**
 * @hidden
 */

const gzip = promisify(gzipCb)

/**
 * @hidden
 */
export default async function marshallCommit(
  commit: Commit,
  asObject?: boolean
): Promise<MarshalledCommitProperty[] | MarshalledCommit> {
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

  const commitObj = {
    s: [aggregateType, aggregateKey].join(':'),
    v: aggregateVersion,
    g: sortKey,
    a: aggregateType,
    r: aggregateVersion === 1 ? aggregateKey : null,
    t: new Date(timestamp).valueOf(),
    e: zippedEvents,
    x: expiresAt || '',
    p: `${new Date(timestamp)
      .toISOString()
      .split('T')[0]
      .replace(/\-/g, '')}${chronologicalGroup}`,
  } as MarshalledCommit

  if (asObject) {
    return commitObj
  }

  return [
    {
      name: 's',
      value: commitObj.s,
    },
    {
      name: 'v',
      value: commitObj.v,
    },
    {
      name: 'g',
      value: commitObj.g,
    },
    {
      name: 'a',
      value: commitObj.a,
    },
    {
      name: 'r',
      value: commitObj.r,
    },
    {
      name: 't',
      value: commitObj.t,
    },
    {
      name: 'e',
      value: commitObj.e,
      excludeFromIndexes: true,
    },
    {
      name: 'x',
      value: commitObj.x,
    },
    {
      name: 'p',
      value: commitObj.p,
    },
  ] as MarshalledCommitProperty[]
}
