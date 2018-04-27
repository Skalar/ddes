/**
 * @module @ddes/event-streaming
 */

import {
  Commit,
  EventWithMetadata,
  Store,
  StorePoller,
  StorePollerParams,
} from '@ddes/core'
import * as debug from 'debug'
import {IncomingMessage} from 'http'
import {get} from 'lodash'
import {Server as WebSocketServer} from 'ws'
import {FilterSet} from './types'

export class EventStreamer extends StorePoller {
  public wss: WebSocketServer

  constructor(
    params: StorePollerParams & {
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
    const {authenticateClient: verifyClient, port = 80, ...rest} = params
    super(rest)

    this.wss = new WebSocketServer({verifyClient, port})
    this.wss.on('connection', this.handleConnection.bind(this))
    this.debug = debug('DDES.EventStreamer.Server')
  }

  public handleConnection(client: any) {
    const clientAddress = client._socket.remoteAddress
    this.debug(`new client (${clientAddress})`)

    if (!this.sortKeyCursor) {
      this.sortKeyCursor = new Date().toISOString().replace(/[^0-9]/g, '')
    }

    this.pollingLoop()

    client.filterSets = []

    client.on('message', (json: string) => {
      this.debug(`filter sets for ${clientAddress} set to ${json}`)
      client.filterSets = JSON.parse(json)
    })

    client.on('close', () => {
      this.debug(`client disconnected (${clientAddress})`)
    })
  }

  public close() {
    if (this.wss) {
      this.wss.close()
    }

    this.stop()
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

  protected get shouldPoll() {
    return this.wss && this.wss.clients.size > 0 && super.shouldPoll
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
}
