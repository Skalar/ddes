import {Pool, PoolClient} from 'pg'
import {sql} from 'pg-sql'
import {EventEmitter} from 'stream'

export class PostgresListener extends EventEmitter {
  protected listeningChannels: Map<string, number> = new Map()
  protected connection?: PoolClient
  protected ended = false
  protected reconnectTimer?: NodeJS.Timeout

  constructor(protected pool: Pool) {
    super()
    this.setMaxListeners(1000)
    this.connect()
  }
  public end() {
    this.ended = true
    this.connection?.release()
  }

  public on(eventName: string | symbol, listener: (...args: any) => void) {
    if (this.ended) throw new Error(`Listener has ended`)
    if (eventName !== 'error' && this.listeners(eventName).length === 0) {
      this.connection?.query(sql`LISTEN ${sql.ident(eventName.toString())}`)
    }
    super.on(eventName, listener)
    return this
  }

  public off(eventName: string | symbol, listener: (...args: any[]) => void) {
    super.off(eventName, listener)
    if (eventName !== 'error' && this.listeners(eventName).length === 0) {
      this.connection?.query(sql`UNLISTEN ${sql.ident(eventName.toString())}`)
    }
    return this
  }

  protected async connect() {
    this.reconnectTimer = undefined

    if (this.ended) return

    try {
      this.connection = await this.pool?.connect()
    } catch (error) {
      this.emit('error', error)
      this.scheduleReconnect()

      return
    }

    this.connection.on('notification', msg => this.emit(msg.channel, msg.payload))
    this.connection.on('error', error => {
      this.emit('error', error)
      this.connection = undefined
      this.scheduleReconnect()
    })

    try {
      await Promise.all(
        this.eventNames()
          .filter(eventName => eventName !== 'error')
          .map(eventName => this.connection?.query(sql`LISTEN ${sql.ident(eventName.toString())}`))
      )
    } catch (error) {
      this.scheduleReconnect()
    }
  }

  protected scheduleReconnect() {
    if (!this.ended && !this.reconnectTimer) {
      this.reconnectTimer = setTimeout(() => this.connect(), 2000)
    }
  }
}
