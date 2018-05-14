/**
 * @module @ddes/event-streaming
 */

import {
  Commit,
  EventWithMetadata,
  StorePoller,
  StorePollerParams,
} from '@ddes/core'
import * as debug from 'debug'
import {IncomingMessage} from 'http'
import {get} from 'lodash'
import {Server as WebSocketServer} from 'ws'
import {FilterSet} from './types'

export default class EventStreamer {
  public wss: WebSocketServer
  protected debug: debug.IDebugger
  protected chronologicalGroups: string[]
  protected storePollers: StorePoller[] = []

  constructor(
    params: StorePollerParams & {
      chronologicalGroups?: string[]
      port: number
      authenticateClient?: (
        info: {
          origin: string
          req: IncomingMessage
          secure: boolean
        }
      ) => boolean
    }
  ) {
    const {
      authenticateClient: verifyClient,
      port = 80,
      chronologicalGroups = ['default'],
      ...storePollerParams
    } = params

    this.wss = new WebSocketServer({verifyClient, port})
    this.wss.on('connection', this.onClientConnected.bind(this))
    this.chronologicalGroups = chronologicalGroups

    for (const chronologicalGroup of chronologicalGroups) {
      this.storePollers.push(
        new StorePoller({
          ...storePollerParams,
          processCommit: this.processCommit.bind(this),
        })
      )
    }

    this.debug = debug('DDES.EventStreamer.Server')
  }

  public close() {
    if (this.wss) {
      this.wss.close()
    }

    for (const storePoller of this.storePollers) {
      storePoller.stop()
    }
  }

  public async processCommit(commit: Commit) {
    const {
      events,
      aggregateType,
      aggregateKey,
      aggregateVersion,
      timestamp,
      sortKey,
    } = commit

    for (const event of events) {
      this.publishEventToSubscribers({
        ...event,
        aggregateType,
        aggregateKey,
        aggregateVersion,
        timestamp,
      })
    }
  }

  public publishEventToSubscribers(eventWithMetadata: EventWithMetadata) {
    clients: for (const client of this.wss.clients) {
      if (!(client as any).filterSets) {
        continue // skip clients that have not sent filtersets yet
      }

      const {filterSets}: {filterSets: FilterSet[]} = client as any

      for (const filterSet of filterSets) {
        for (const [filterKey, filterValue] of Object.entries(filterSet)) {
          const eventValue = get(eventWithMetadata, filterKey)
          if (Array.isArray(filterValue)) {
            if (!filterValue.includes(eventValue)) {
              continue clients
            }
          } else if (typeof filterValue === 'object' && filterValue.regexp) {
            if (
              !(
                typeof eventValue === 'string' &&
                eventValue.match(filterValue.regexp)
              )
            ) {
              continue clients
            }
          } else {
            if (eventValue !== filterValue) {
              continue clients
            }
          }
        }
      }
      if (client.OPEN) {
        client.send(JSON.stringify(eventWithMetadata))
      }
    }
  }

  protected onClientConnected(client: any) {
    const clientAddress = client._socket.remoteAddress
    this.debug(`new client (${clientAddress})`)

    client.filterSets = []

    client.on('message', (json: string) => {
      this.debug(`filter sets for ${clientAddress} set to ${json}`)
      client.filterSets = JSON.parse(json)
    })

    client.on('close', this.onClientDisconnected.bind(this))

    if (this.wss.clients.size === 1) {
      for (const storePoller of this.storePollers) {
        storePoller.sortKeyCursor = new Date()
        storePoller.start()
      }
    }
  }

  protected onClientDisconnected() {
    if (this.wss.clients.size === 0) {
      for (const storePoller of this.storePollers) {
        storePoller.stop()
      }
    }
  }
}
