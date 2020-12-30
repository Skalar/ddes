/**
 * @module @ddes/event-streaming
 */

import {EventEmitter} from 'events'
import WebSocket from 'ws'
import {FilterSet} from './types'

export default class EventSubscriber extends EventEmitter {
  public maxQueueSize = 1000

  private socket?: WebSocket
  private queue: object[] = []
  public isReady: Promise<boolean>
  public ready = false

  constructor(
    params: {
      wsUrl: string
      events: FilterSet[]
      maxQueueSize?: number
    } & WebSocket.ClientOptions
  ) {
    super()

    const {wsUrl, maxQueueSize, events: filterSets, ...websocketClientOptions} = params

    if (maxQueueSize) {
      this.maxQueueSize = maxQueueSize
    }

    // Prevent warning
    this.setMaxListeners(0)

    this.socket = new WebSocket(params.wsUrl, websocketClientOptions)
    this.isReady = new Promise(resolve => {
      this.socket!.on('open', () => {
        // Tell the server which events we care about
        this.socket!.send(JSON.stringify(filterSets))
      })

      this.socket!.once('message', message => {
        if (message === 'READY') {
          this.ready = true
          this.socket!.on('message', json => {
            if (this.ready) {
              const event = JSON.parse(json as string)
              this.emit('event', event)
              if (this.queue.length < this.maxQueueSize) {
                this.queue.push(event)
              } else {
                this.emit('error', new Error(`Max queue size of ${this.maxQueueSize} reached, dropping event`))
              }
            }
          })
          resolve(true)
        }
      })
    })

    this.socket.on('error', () => {
      this.socket = undefined
    })
    this.socket.on('close', () => {
      this.emit('close')
      this.socket = undefined
    })
  }

  public close() {
    return this.socket && this.socket.close()
  }

  /**
   * @hidden
   */
  public [Symbol.asyncIterator]() {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const eventStream = this

    if (!this.socket) {
      throw new Error('No active socket')
    }

    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const subscriber = this

    return {
      async next() {
        while (true) {
          if (!eventStream.socket) {
            return {value: undefined, done: true}
          }

          if (subscriber.queue.length) {
            return {value: subscriber.queue.shift(), done: false}
          }

          await Promise.race([
            new Promise(resolve => subscriber.socket!.once('message', resolve)),
            new Promise(resolve => subscriber.socket!.once('close', resolve)),
            new Promise(resolve => subscriber.socket!.once('error', resolve)),
          ])
        }
      },

      async return() {
        eventStream.close()

        return {value: undefined, done: true}
      },

      async throw(error: Error) {
        eventStream.close()

        throw error
      },
    }
  }
}
