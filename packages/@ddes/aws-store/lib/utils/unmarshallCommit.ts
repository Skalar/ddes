/**
 * @module @ddes/aws-store
 */

import {Commit, Event} from '@ddes/core'
import {DynamoDB} from 'aws-sdk'
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
  marshalledCommit: MarshalledCommit
): Promise<Commit> {
  const unmarshalled = DynamoDB.Converter.unmarshall(marshalledCommit)
  const [, aggregateType, aggregateKey] = unmarshalled.s.match(/^([^:]*):(.*)$/)

  const commit = new Commit({
    aggregateType,
    aggregateKey,
    aggregateVersion: unmarshalled.v,
    expiresAt: unmarshalled.x,
    timestamp: unmarshalled.t,
    events: JSON.parse((await gunzip(unmarshalled.e)) as string).map(
      ({
        t: type,
        v: version = 1,
        p: properties,
      }: {
        t: string
        v: number
        p: object
      }) =>
        ({
          type,
          version,
          properties,
        } as Event)
    ),
    chronologicalGroup: unmarshalled.p.substr(8, unmarshalled.p.length - 8),
    storeKey: {
      s: marshalledCommit.s,
      v: marshalledCommit.v,
    },
  })

  return commit
}
