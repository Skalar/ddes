import {Pool, PoolClient} from 'pg'
import {sql} from 'pg-sql'
import {EventEmitter} from 'stream'

export class PostgresListener extends EventEmitter {
  protected listeningChannels: Map<string, number> = new Map()
  protected connection?: PoolClient
  protected ended = false

  protected reconnectDelay?: number
  protected reconnectTimer?: NodeJS.Timeout
  protected connectionHealthCheckInterval?: number
  protected connectionHealthCheckTimer?: NodeJS.Timer

  constructor(
    protected pool: Pool,
    options: {
      /**
       * Frequency of connection health checks in seconds (default: 30)
       */
      connectionHealthCheckInterval?: number

      /**
       * Number of seconds to wait before attempting to reconnect to postgres (default: 2)
       */
      reconnectDelay?: number
    } = {}
  ) {
    super()
    this.setMaxListeners(1000)
    this.connectionHealthCheckInterval = options.connectionHealthCheckInterval ?? 30
    this.reconnectDelay = options.reconnectDelay ?? 2

    this.connect()
  }

  public end() {
    this.ended = true
    this.handleDisconnect()
  }

  public on(eventName: string | symbol, listener: (...args: any) => void) {
    if (this.ended) throw new Error(`Listener has ended`)
    if (eventName !== 'connectionError' && this.listeners(eventName).length === 0) {
      this.connection?.query(sql`LISTEN ${sql.ident(eventName.toString())}`)
    }
    super.on(eventName, listener)
    return this
  }

  public off(eventName: string | symbol, listener: (...args: any[]) => void) {
    super.off(eventName, listener)
    if (eventName !== 'connectionError' && this.listeners(eventName).length === 0) {
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
      this.emit('connectionError', error)
      this.handleDisconnect()

      return
    }

    this.connection.on('notification', msg => this.emit(msg.channel, msg.payload))
    this.connection.on('end', () => this.handleDisconnect())
    this.connection.on('error', error => {
      this.emit('connectionError', error)
      this.handleDisconnect()
    })

    if (this.connectionHealthCheckInterval) {
      this.connectionHealthCheckTimer = setInterval(
        () => this.verifyConnectionHealth(),
        this.connectionHealthCheckInterval * 1000
      )
    }

    try {
      await Promise.all(
        this.eventNames()
          .filter(eventName => eventName !== 'error')
          .map(eventName => this.connection?.query(sql`LISTEN ${sql.ident(eventName.toString())}`))
      )
    } catch (error) {
      this.handleDisconnect()
    }
  }

  protected async verifyConnectionHealth() {
    if (!this.connection) throw new Error(`Trying to verify connection health when no connection`)

    try {
      await this.connection.query('SELECT pg_backend_pid()')
    } catch (error) {
      this.emit('connectionError', new Error(`Postgres connection health check failed: ${error}`))
      this.handleDisconnect()
    }
  }

  protected handleDisconnect() {
    if (this.connectionHealthCheckTimer) clearInterval(this.connectionHealthCheckTimer)
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer)

    if (this.connection) {
      try {
        this.connection.release()
      } catch {
        // in case already released
      }
    }

    this.connection = undefined

    if (!this.ended && this.reconnectDelay) {
      this.reconnectTimer = setTimeout(() => this.connect(), this.reconnectDelay)
    }
  }
}
