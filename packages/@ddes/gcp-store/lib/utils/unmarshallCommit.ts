/**
 * @module @ddes/gcp-store
 */

import {Commit, Event} from '@ddes/core'
import {promisify} from 'util'
import {gunzip as gunzipCb} from 'zlib'
import {MarshalledCommit} from '../types'

/**
 * @hidden
 */
const gunzip = promisify(gunzipCb)

/**
 * @hidden
 */

export default async function unmarshallCommit(
  marshalled: MarshalledCommit
): Promise<Commit> {
  const [, aggregateType, aggregateKey] = (marshalled.s as any).match(
    /^([^:]*):(.*)$/
  )

  const commit = new Commit({
    aggregateType,
    aggregateKey,
    aggregateVersion: parseInt(marshalled.v, 10),
    expiresAt: parseInt(marshalled.x, 10),
    timestamp: marshalled.t,
    events: JSON.parse((await gunzip(marshalled.e)) as string).map(
      ({
        t: type,
        v: version = 1,
        p: properties,
      }: {
        t: string
        v: number
        p: object
      }) => ({type, version, properties} as Event)
    ),
    chronologicalGroup: marshalled.p.substr(8, marshalled.p.length - 8),
    storeKey: {
      s: marshalled.s,
      v: marshalled.v,
    },
  })

  return commit
}
